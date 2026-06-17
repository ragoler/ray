#!/usr/bin/env bash
# Standalone provisioning for the Ray Render Farm. The Hub IGNORES this file —
# it applies cluster/ and infra/ itself. Use this only to run the demo on your
# own GKE cluster without the Hub.
set -euo pipefail

: "${PROJECT_NAME:?set PROJECT_NAME}"
: "${REGION:?set REGION}"
: "${ARTIFACT_REGISTRY_REPO:?set ARTIFACT_REGISTRY_REPO}"
NAMESPACE="${NAMESPACE:-default}"
RAY_IMAGE="${RAY_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_NAME}/${ARTIFACT_REGISTRY_REPO}/ray-render-farm:latest}"
WORKER_MIN_REPLICAS="${WORKER_MIN_REPLICAS:-0}"
WORKER_MAX_REPLICAS="${WORKER_MAX_REPLICAS:-6}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Building and pushing image: ${RAY_IMAGE}"
docker build -t "${RAY_IMAGE}" -f "${ROOT}/app/Dockerfile" "${ROOT}/app"
docker push "${RAY_IMAGE}"

echo "==> Cluster-scoped prerequisites (KubeRay operator + Spot ComputeClass)"
kubectl apply -k "${ROOT}/cluster/kuberay-operator"
kubectl apply -f "${ROOT}/cluster/spot-computeclass.yaml"

echo "==> Waiting for the KubeRay operator to be ready"
kubectl -n ray-system rollout status deploy/kuberay-operator --timeout=180s || true

echo "==> Deploying per-namespace infra into: ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
for f in "${ROOT}"/infra/*.yaml; do
  echo "    applying $(basename "$f")"
  sed -e "s|\${NAMESPACE}|${NAMESPACE}|g" \
      -e "s|\${RAY_IMAGE}|${RAY_IMAGE}|g" \
      -e "s|\${WORKER_MIN_REPLICAS}|${WORKER_MIN_REPLICAS}|g" \
      -e "s|\${WORKER_MAX_REPLICAS}|${WORKER_MAX_REPLICAS}|g" \
      "$f" | kubectl apply -n "${NAMESPACE}" -f -
done

echo "==> Done. Gateway IP (may take a few minutes):"
kubectl -n "${NAMESPACE}" get gateway ray-gateway -o jsonpath='{.status.addresses[0].value}{"\n"}' || true
