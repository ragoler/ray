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

# --reset-gateway: delete the Gateway + HTTPRoute before applying, forcing the
# GKE controller to reconcile them fresh. Use this if the Gateway is wedged
# (e.g. stuck "Waiting for controller" after a class change left a stale route).
RESET_GW=false
case "${1:-}" in
  --reset-gateway) RESET_GW=true ;;
  -h|--help)       echo "Usage: $0 [--reset-gateway]"; exit 0 ;;
  "")              ;;
  *) echo "Unknown argument: $1 (use --reset-gateway or no flag)"; exit 1 ;;
esac

# Per-cluster image tag so multiple clusters never clobber each other's image.
IMAGE_TAG="${IMAGE_TAG:-${CLUSTER_NAME}}"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}"
RAY_IMAGE="${REGISTRY}/ray-render-farm:${IMAGE_TAG}"

# Source of truth for the gateway name is the manifest, not .env (which can drift
# from the deployed name and make IP discovery look up the wrong gateway).
GATEWAY_NAME=$(awk '/kind: Gateway/{f=1} f&&/^  name:/{print $2; exit}' "${ROOT}/infra/gateway.yaml")

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
# Context is the repo root so the image can include both app/ and frontend/.
docker build --platform linux/amd64 -t "${RAY_IMAGE}" -f "${ROOT}/app/Dockerfile" "${ROOT}"

echo "=== Pushing image ==="
docker push "${RAY_IMAGE}"

echo "=== Deploying per-namespace infra into: ${NAMESPACE} ==="
# Create the namespace only if missing (avoids the kubectl-apply annotation
# warning on pre-existing namespaces like "default").
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${NAMESPACE}"

# Force-delete the Gateway, stripping a stuck finalizer if its controller never
# adopted it (e.g. after a class migration left it wedged "Waiting for
# controller"). Safe here: a wedged Gateway has no GCP resources to orphan.
force_delete_gateway() {
  local gw="$1"
  kubectl -n "${NAMESPACE}" get gateway "${gw}" >/dev/null 2>&1 || return 0
  kubectl -n "${NAMESPACE}" delete gateway "${gw}" --ignore-not-found --timeout=60s && return 0
  echo "Gateway delete is stuck; removing finalizer..."
  kubectl -n "${NAMESPACE}" patch gateway "${gw}" --type=merge -p '{"metadata":{"finalizers":null}}' || true
  kubectl -n "${NAMESPACE}" wait --for=delete "gateway/${gw}" --timeout=60s || true
}

# Recreate the Gateway + HTTPRoute together when the class changed (gatewayClassName
# is immutable) or on --reset-gateway. Recreate the route too, else its status
# stays stale and the new Gateway never programs.
DESIRED_GW_CLASS=$(grep -m1 'gatewayClassName:' "${ROOT}/infra/gateway.yaml" | awk '{print $2}')
CUR_GW_CLASS=$(kubectl -n "${NAMESPACE}" get gateway "${GATEWAY_NAME}" -o jsonpath='{.spec.gatewayClassName}' 2>/dev/null || true)
if [ "${RESET_GW}" = true ] || { [ -n "${CUR_GW_CLASS}" ] && [ "${CUR_GW_CLASS}" != "${DESIRED_GW_CLASS}" ]; }; then
  echo "Recreating Gateway + HTTPRoute for a clean reconcile (its IP will change)."
  kubectl -n "${NAMESPACE}" delete httproute ray-route --ignore-not-found
  force_delete_gateway "${GATEWAY_NAME}"
fi

# One-time migration: remove the legacy classic gxlb gateway (replaced by the
# dedicated modern gateway). No-op once it's gone.
[ "${GATEWAY_NAME}" != "ray-gateway" ] && force_delete_gateway "ray-gateway"

# Variables the infra manifests reference.
export NAMESPACE RAY_IMAGE WORKER_MIN_REPLICAS WORKER_MAX_REPLICAS
for f in "${ROOT}"/infra/*.yaml; do
  echo "    applying $(basename "$f")"
  render "$f" | kubectl apply -n "${NAMESPACE}" -f -
done

echo "=== Restarting Ray pods to pick up the latest image ==="
# KubeRay does NOT recreate head/worker pods when the RayCluster spec is re-applied,
# so the head would keep running a stale image (and stale task code). Delete the Ray
# pods; KubeRay recreates the head (imagePullPolicy: Always pulls the rebuild).
# Workers are autoscaled from 0, so there are usually none to delete.
kubectl -n "${NAMESPACE}" delete pod -l ray.io/cluster=ray-render-farm --ignore-not-found
sleep 10
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod \
  -l ray.io/cluster=ray-render-farm,ray.io/node-type=head --timeout=300s || true

echo "=== Rolling out the controller ==="
# Force a fresh pull of the rebuilt image (stable per-cluster tag + Always policy).
# After the head restart so the controller's Ray Client connects to the new head.
kubectl -n "${NAMESPACE}" rollout restart deployment/ray-controller-deployment
kubectl -n "${NAMESPACE}" rollout status deployment/ray-controller-deployment --timeout=600s || true

echo "=== Deployed. Discovering Gateway IP (may take 3-5 minutes) ==="
# Prefer the Gateway status; fall back to the GCP forwarding rule (named after the
# gateway: gkegw1-<hash>-<namespace>-<gateway-name>-<hash>).
gateway_ip() {
  local ip
  ip=$(kubectl -n "${NAMESPACE}" get gateway "${GATEWAY_NAME}" -o jsonpath='{.status.addresses[0].value}' 2>/dev/null || true)
  [ -z "${ip}" ] && ip=$(gcloud compute forwarding-rules list --global --project="${PROJECT_ID}" \
    --filter="name~gkegw1.*-${NAMESPACE}-${GATEWAY_NAME}" --format="value(IPAddress)" 2>/dev/null | head -1)
  echo "${ip}"
}
for i in {1..30}; do
  GATEWAY_IP=$(gateway_ip)
  if [ -n "${GATEWAY_IP}" ]; then
    echo "Gateway IP: ${GATEWAY_IP}"
    echo "  Demo API:  http://${GATEWAY_IP}/healthz"
    break
  fi
  sleep 10
done
[ -z "${GATEWAY_IP:-}" ] && echo "Gateway IP not ready yet; check: kubectl -n ${NAMESPACE} get gateway ${GATEWAY_NAME}"

DASH_IP=$(kubectl -n "${NAMESPACE}" get svc ray-dashboard -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
[ -n "${DASH_IP}" ] && echo "  Ray Dashboard: http://${DASH_IP}/" \
  || echo "  Ray Dashboard: provisioning (kubectl -n ${NAMESPACE} get svc ray-dashboard)"
echo "=== Done. Run ./verify_setup.sh to smoke-test. ==="
