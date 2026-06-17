#!/usr/bin/env bash
# Standalone provisioning for the Ray Render Farm: GKE cluster + cluster-scoped
# prerequisites (KubeRay operator + Spot ComputeClass). Run deploy_app.sh after
# this to build/push the image and deploy the RayCluster + controller.
#
# The Hub IGNORES this file — it assumes a live cluster, installs cluster/ during
# build_infra.sh, and applies infra/ per deploy.
set -e

# --- Load configuration ----------------------------------------------------
if [ -f .env ]; then
  source .env
else
  echo "Error: .env file not found. Create one with: cp .env.example .env"
  exit 1
fi

for cmd in gcloud kubectl python3; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: $cmd is required but not installed."
    exit 1
  fi
done

REGION="${REGION:-${ZONE%-*}}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Portable ${VAR} substitution for manifests (envsubst isn't on stock macOS).
# Leaves $(VAR) alone so Kubernetes downward-API refs survive to runtime.
render() { python3 -c "import os,sys;sys.stdout.write(os.path.expandvars(open(sys.argv[1]).read()))" "$1"; }

# --- Mode dispatch ---------------------------------------------------------
#   (no flag)         create cluster + prerequisites
#   --delete          remove cluster-scoped prereqs (keep the cluster)
#   --delete-cluster  the above, plus delete the GKE cluster
MODE="create"
case "${1:-}" in
  --delete)         MODE="delete" ;;
  --delete-cluster) MODE="delete-cluster" ;;
  -h|--help)        echo "Usage: $0 [--delete | --delete-cluster]"; exit 0 ;;
  "")               MODE="create" ;;
  *) echo "Unknown argument: $1 (use --delete, --delete-cluster, or no flag)"; exit 1 ;;
esac

cluster_exists() {
  gcloud container clusters describe "${CLUSTER_NAME}" \
    --zone="${ZONE}" --project="${PROJECT_ID}" &>/dev/null
}

if [ "$MODE" = "delete" ] || [ "$MODE" = "delete-cluster" ]; then
  if cluster_exists; then
    gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}"
    echo "=== Removing cluster-scoped prerequisites ==="
    kubectl delete -f "${ROOT}/cluster/spot-computeclass.yaml" --ignore-not-found || true
    kubectl delete -k "${ROOT}/cluster/kuberay-operator" --ignore-not-found || true
  else
    echo "Cluster ${CLUSTER_NAME} does not exist; nothing to remove."
  fi
  if [ "$MODE" = "delete-cluster" ] && cluster_exists; then
    echo "=== Deleting GKE cluster ${CLUSTER_NAME} (several minutes) ==="
    gcloud container clusters delete "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}" --quiet || true
  fi
  echo "=== Teardown complete ==="
  exit 0
fi

# --- Step 1: Create the GKE cluster ---------------------------------------
# Node Auto-Provisioning is required so the ray-spot ComputeClass can create
# Spot node pools on demand for the Ray workers. The small default pool hosts
# the KubeRay operator, the Ray head, and the controller.
echo "=== Step 1: Creating GKE cluster ${CLUSTER_NAME} (${ZONE}) ==="
if cluster_exists; then
  echo "Cluster ${CLUSTER_NAME} already exists. Skipping creation."
else
  gcloud container clusters create "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --machine-type="${MACHINE_TYPE}" \
    --num-nodes="${NUM_NODES}" \
    --gateway-api=standard \
    --enable-autoprovisioning \
    --min-cpu 0 --max-cpu "${MAX_CPU:-200}" \
    --min-memory 0 --max-memory "${MAX_MEMORY:-800}"
fi

echo "=== Step 2: Getting cluster credentials ==="
gcloud container clusters get-credentials "${CLUSTER_NAME}" --project="${PROJECT_ID}" --zone="${ZONE}"

# --- Step 3: KubeRay operator (pinned kustomize) --------------------------
echo "=== Step 3: Installing the KubeRay operator (cluster-scoped, pinned) ==="
# The kustomization pins the operator to ray-system; kubectl apply won't create
# the namespace, so ensure it exists first.
kubectl get namespace ray-system >/dev/null 2>&1 || kubectl create namespace ray-system
kubectl apply --server-side -k "${ROOT}/cluster/kuberay-operator"
# One-time migration safeguard: an earlier KubeRay version installed the operator
# in the default namespace. Remove any stray duplicate so exactly one operator
# reconciles RayClusters (two would fight over creating worker pods). No-op on a
# clean cluster.
kubectl delete deployment kuberay-operator -n default --ignore-not-found
kubectl -n ray-system rollout status deploy/kuberay-operator --timeout=300s || true

# --- Step 4: Spot ComputeClass --------------------------------------------
echo "=== Step 4: Applying the Spot ComputeClass for Ray workers ==="
kubectl apply -f "${ROOT}/cluster/spot-computeclass.yaml"

echo "=== Setup complete. Next: ./deploy_app.sh ==="
