# Single image for BOTH the controller Deployment and the RayCluster head/worker
# pods. Basing on the official Ray image guarantees the Ray Client (driver) and
# the cluster run the exact same Ray version.
FROM rayproject/ray:2.40.0-py311

WORKDIR /app

# Ray + numpy are already in the base image; add only the controller extras.
# (Kept separate from requirements.txt so we don't reinstall/risk bumping Ray.)
RUN pip install --no-cache-dir \
    "fastapi==0.115.6" \
    "uvicorn[standard]==0.34.0" \
    "pydantic==2.10.4" \
    "pillow==11.0.0" \
    "kubernetes==31.0.0"

# Application code. The base image runs as the non-root `ray` user (uid 1000,
# gid 100/users), so copy the files owned by ray — otherwise they land as root
# and ray can't read them (uvicorn/import fails with PermissionError).
# PYTHONPATH=/app makes `tasks` importable on the Ray workers (same image), so
# render_tile is available cluster-wide.
COPY --chown=ray:users tasks.py controller.py ./
ENV PYTHONPATH=/app

EXPOSE 8000

# Default entrypoint = the controller. KubeRay overrides the command for the Ray
# head/worker pods, so this CMD only applies to the controller Deployment.
CMD ["uvicorn", "controller:app", "--host", "0.0.0.0", "--port", "8000"]
