"""Manifest + descriptor checks: every ${VAR} is declared, refs are consistent.

Mirrors the Hub pre-merge checklist so a broken descriptor fails locally.
"""

import pathlib
import re

import pytest

yaml = pytest.importorskip("yaml")

ROOT = pathlib.Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra"

# Hub-provided variables (see feature.md §3).
HUB_VARS = {
    "NAMESPACE", "PROJECT_NAME", "REGION", "ARTIFACT_REGISTRY_REPO",
    "GOOGLE_GENAI_USE_VERTEXAI", "OPENAI_API_BASE", "GCS_MODEL_BUCKET",
}


def _feature():
    return yaml.safe_load((ROOT / "feature.yaml").read_text())


def test_feature_yaml_required_keys():
    f = _feature()
    for key in ("name", "paths", "deployment_name", "gateway"):
        assert key in f, f"feature.yaml missing {key}"
    assert f["name"] == "ray"
    assert f["gateway"]["name"] == "ray-gateway"


def test_exactly_one_ui_model():
    f = _feature()
    has_playroom = "frontend_dir" in f["paths"] and "playroom_slug" in f["paths"]
    has_linkout = "entrypoint_service" in f
    assert has_playroom and not has_linkout, "must be hub-hosted playroom only"


def test_every_var_is_hub_standard_or_defaulted():
    """Grep all manifests for ${VAR}; each must be Hub-standard or defaulted."""
    f = _feature()
    declared = HUB_VARS | set(f.get("template_defaults", {}).keys())
    pattern = re.compile(r"\$\{([A-Z_]+)\}")
    missing = {}
    for path in list(INFRA.glob("*.yaml")) + list((ROOT / "cluster").rglob("*.yaml")):
        for var in pattern.findall(path.read_text()):
            if var not in declared:
                missing.setdefault(path.name, set()).add(var)
    assert not missing, f"undeclared template vars: {missing}"


def test_deployment_name_matches_descriptor():
    f = _feature()
    name = f["deployment_name"]
    found = False
    for path in INFRA.glob("*.yaml"):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc and doc.get("kind") == "Deployment" and doc["metadata"]["name"] == name:
                found = True
    assert found, f"no Deployment named {name}"


def test_gateway_name_matches_descriptor():
    f = _feature()
    gw = f["gateway"]["name"]
    names = []
    for path in INFRA.glob("*.yaml"):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc and doc.get("kind") == "Gateway":
                names.append(doc["metadata"]["name"])
    assert gw in names, f"Gateway {gw} not found (found {names})"


def test_no_hardcoded_default_namespace():
    """Resource namespaces must be templated, never literally 'default'."""
    for path in INFRA.glob("*.yaml"):
        for doc in yaml.safe_load_all(path.read_text()):
            if not doc:
                continue
            ns = doc.get("metadata", {}).get("namespace")
            if ns is not None:
                assert ns == "${NAMESPACE}", f"{path.name}: hardcoded namespace {ns}"


def test_httproute_backends_and_dashboard_route():
    route = None
    for doc in yaml.safe_load_all((INFRA / "http-route.yaml").read_text()):
        if doc and doc.get("kind") == "HTTPRoute":
            route = doc
    assert route is not None
    backends = {
        b["name"]
        for rule in route["spec"]["rules"]
        for b in rule.get("backendRefs", [])
    }
    assert "ray-controller" in backends
    assert "ray-render-farm-head-svc" in backends  # dashboard route
    # Dashboard rule rewrites the prefix to '/'.
    dash = [r for r in route["spec"]["rules"]
            if any(m["path"]["value"] == "/ray-dashboard" for m in r["matches"])]
    assert dash and dash[0]["filters"][0]["urlRewrite"]["path"]["replacePrefixMatch"] == "/"


def test_raycluster_autoscaling_and_spot():
    rc = None
    for doc in yaml.safe_load_all((INFRA / "raycluster.yaml").read_text()):
        if doc and doc.get("kind") == "RayCluster":
            rc = doc
    assert rc is not None
    assert rc["spec"]["enableInTreeAutoscaling"] is True
    worker = rc["spec"]["workerGroupSpecs"][0]
    assert worker["minReplicas"] == "${WORKER_MIN_REPLICAS}"
    assert worker["maxReplicas"] == "${WORKER_MAX_REPLICAS}"
    sel = worker["template"]["spec"]["nodeSelector"]
    assert sel["cloud.google.com/compute-class"] == "ray-spot"
    # POD_NAME via downward API for tile attribution.
    env = worker["template"]["spec"]["containers"][0]["env"]
    assert any(e["name"] == "POD_NAME" for e in env)
