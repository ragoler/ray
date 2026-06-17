#!/usr/bin/env bash
# Build & push the Ray Render Farm image, then deploy the per-namespace infra
# (RayCluster + controller + Gateway + HTTPRoute). Run setup_infra.sh first.
#
# The Hub IGNORES this file — it builds images from feature.yaml `build:` entries
# and applies infra/ itself.
set -e

if [ -f .env ]; then
  source .env
else
  echo "Error: .env file not found. Create one with: cp .env.example .env"
  exit 1
fi

REGION="${REGION:-${ZONE%-*}}"
NAMESPACE="${NAMESPACE:-default}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Per-cluster image tag so multiple clusters never clobber each other's image.
IMAGE_TAG="${IMAGE_TAG:-${CLUSTER_NAME}}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}"
RAY_IMAGE="${REGISTRY}/ray-render-farm:${IMAGE_TAG}"

# Portable ${VAR} substitution (leaves $(VAR) downward-API refs intact).
render() { python3 -c "import os,sys;sys.stdout.write(os.path.expandvars(open(sys.argv[1]).read()))" "$1"; }

echo "=== Targeting cluster ${CLUSTER_NAME} (${ZONE}) ==="
gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}"

echo "=== Ensuring Artifact Registry repo ${ARTIFACT_REGISTRY_REPO} exists ==="
gcloud artifacts repositories create "${ARTIFACT_REGISTRY_REPO}" \
  --repository-format=docker --location="${REGION}" \
  --description="Ray Render Farm images" --project="${PROJECT_ID}" \
  || echo "Repo may already exist; continuing."

echo "=== Authenticating Docker to ${REGION}-docker.pkg.dev ==="
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "=== Building image (linux/amd64 for GKE nodes): ${RAY_IMAGE} ==="
docker build --platform linux/amd64 -t "${RAY_IMAGE}" -f "${ROOT}/app/Dockerfile" "${ROOT}/app"

echo "=== Pushing image ==="
docker push "${RAY_IMAGE}"

echo "=== Deploying per-namespace infra into: ${NAMESPACE} ==="
# Create the namespace only if missing (avoids the kubectl-apply annotation
# warning on pre-existing namespaces like "default").
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${NAMESPACE}"

# gatewayClassName is immutable; if it changed, recreate the Gateway (its IP
# will change). No-op on first deploy or when the class is unchanged.
DESIRED_GW_CLASS=$(grep -m1 'gatewayClassName:' "${ROOT}/infra/gateway.yaml" | awk '{print $2}')
CUR_GW_CLASS=$(kubectl -n "${NAMESPACE}" get gateway "${GATEWAY_NAME}" -o jsonpath='{.spec.gatewayClassName}' 2>/dev/null || true)
if [ -n "${CUR_GW_CLASS}" ] && [ "${CUR_GW_CLASS}" != "${DESIRED_GW_CLASS}" ]; then
  echo "Gateway class changed (${CUR_GW_CLASS} -> ${DESIRED_GW_CLASS}); recreating Gateway."
  kubectl -n "${NAMESPACE}" delete gateway "${GATEWAY_NAME}" --ignore-not-found
fi

# Variables the infra manifests reference.
export NAMESPACE RAY_IMAGE WORKER_MIN_REPLICAS WORKER_MAX_REPLICAS
for f in "${ROOT}"/infra/*.yaml; do
  echo "    applying $(basename "$f")"
  render "$f" | kubectl apply -n "${NAMESPACE}" -f -
done

echo "=== Rolling out the controller ==="
# Force a fresh pull of the rebuilt image (stable per-cluster tag + Always policy).
kubectl -n "${NAMESPACE}" rollout restart deployment/ray-controller-deployment
kubectl -n "${NAMESPACE}" rollout status deployment/ray-controller-deployment --timeout=600s || true

echo "=== Deployed. Discovering Gateway IP (may take 3-5 minutes) ==="
for i in {1..30}; do
  GATEWAY_IP=$(kubectl -n "${NAMESPACE}" get gateway "${GATEWAY_NAME}" -o jsonpath='{.status.addresses[0].value}' 2>/dev/null || true)
  if [ -n "${GATEWAY_IP}" ]; then
    echo "Gateway IP: ${GATEWAY_IP}"
    echo "  Demo API:      http://${GATEWAY_IP}/healthz"
    echo "  Ray Dashboard: http://${GATEWAY_IP}/ray-dashboard"
    break
  fi
  sleep 10
done
[ -z "${GATEWAY_IP:-}" ] && echo "Gateway IP not ready yet; check: kubectl -n ${NAMESPACE} get gateway ${GATEWAY_NAME}"
echo "=== Done. Run ./verify_setup.sh to smoke-test. ==="
