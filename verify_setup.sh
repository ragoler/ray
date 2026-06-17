#!/usr/bin/env bash
# Post-deployment validation for the Ray Render Farm: waits for the controller
# and Ray head, discovers the Gateway IP, and smoke-tests the data plane.
set -e

if [ -f .env ]; then
  source .env
else
  echo "Error: .env file not found."
  exit 1
fi
NAMESPACE="${NAMESPACE:-default}"

echo "=== Targeting cluster ${CLUSTER_NAME} (${ZONE}) ==="
gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone="${ZONE}" --project="${PROJECT_ID}"

echo "=== Waiting for the controller and Ray head to be Ready ==="
kubectl -n "${NAMESPACE}" rollout status deployment/ray-controller-deployment --timeout=300s
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod \
  -l ray.io/cluster=ray-render-farm,ray.io/node-type=head --timeout=600s

echo "=== Discovering Gateway IP ==="
for i in {1..30}; do
  GATEWAY_IP=$(kubectl -n "${NAMESPACE}" get gateway "${GATEWAY_NAME}" -o jsonpath='{.status.addresses[0].value}' 2>/dev/null || true)
  [ -n "${GATEWAY_IP}" ] && break
  sleep 10
done
if [ -z "${GATEWAY_IP:-}" ]; then
  echo "Error: Gateway did not receive an IP within 5 minutes."
  exit 1
fi
echo "Gateway IP: ${GATEWAY_IP}"

BASE="http://${GATEWAY_IP}"

echo "=== Health check ==="
curl -fsS "${BASE}/healthz" && echo

echo "=== Presets ==="
curl -fsS "${BASE}/presets" >/dev/null && echo "presets OK"

echo "=== Launching a small render (256px) ==="
JOB=$(curl -fsS -X POST "${BASE}/render" \
  -H 'Content-Type: application/json' \
  -d '{"preset":"overview","resolution":256,"max_iter":128}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['job_id'])")
echo "job_id=${JOB}"

echo "=== Streaming tiles (expect meta + tiles + done) ==="
# -N: no buffering; -m: cap at 120s in case the cluster is cold (scaling Spot).
TILES=$(curl -fsS -N -m 120 "${BASE}/render/${JOB}/stream" | grep -c '"type": "tile"' || true)
echo "tiles streamed: ${TILES}"
[ "${TILES}" -ge 1 ] || { echo "Error: no tiles streamed."; exit 1; }

echo "=== Cluster map (workers endpoint) ==="
curl -fsS "${BASE}/workers" | python3 -c "import sys,json;d=json.load(sys.stdin);print('pods:',[p['pod_name'] for p in d['pods']])"

echo "=== Verification successful ==="
DASH_IP=$(kubectl -n "${NAMESPACE}" get svc ray-dashboard -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
[ -n "${DASH_IP}" ] && echo "Open the Ray Dashboard at: http://${DASH_IP}/" \
  || echo "Ray Dashboard LoadBalancer still provisioning (kubectl -n ${NAMESPACE} get svc ray-dashboard)"
