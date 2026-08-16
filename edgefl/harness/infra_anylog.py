"""AnyLog infra backend: the PRIMARY deployment path on Apple Silicon
(AnylogNetworkSetup/AnyLog-Setup.md). Drives the AnyLog-co/docker-compose repo
(branch roy-local) instead of the EdgeLake Makefile.

Differences from the EdgeLake backend (infra.py) that make this one simpler:
  - Containers run with Docker Desktop HOST networking, so every node and Postgres
    shard is 127.0.0.1:<port> from everywhere — no bridge-IP discovery, no
    LEDGER_CONN/DB_IP rewriting per boot.
  - Requires the ANYLOG_LICENSE env var (docker compose >= 2.24 interpolates
    $ANYLOG_LICENSE inside the master's base_configs.env). A stale/expired license
    shows up as the master refusing to start its processes.

Per-operator config dirs (docker-makefiles/operator<i>-configs/) are generated from
operator1's committed config, patching name/ports/DB_PORT and giving EVERY operator its
own CLUSTER_NAME — operators sharing a cluster replicate each other's data, which would
silently break per-Node data-shard isolation (the committed operator2/3 demo configs
share "NYC-branch" for exactly that HA behavior; benchmarks must not).
"""

import os
import re
import shutil
import subprocess

from platform_components.lib.logger.error_handling import get_logger
from .identity import NodeIdentity, master_ports, node_identities
from .infra import psql_host_port, start_postgres, stop_postgres
from .readiness import wait_edgelake_process, wait_edgelake_status

logger = get_logger(__name__)


def _compose_dir(host) -> str:
    d = getattr(host, "ANYLOG_COMPOSE_DIR", None)
    if not d or not os.path.isdir(os.path.join(d, "docker-makefiles")):
        raise RuntimeError(
            "host_profile.ANYLOG_COMPOSE_DIR must point at a checkout of "
            "github.com/AnyLog-co/docker-compose (branch roy-local). "
            "See AnylogNetworkSetup/AnyLog-Setup.md sections 3-5."
        )
    return d


def require_license() -> None:
    if not os.environ.get("ANYLOG_LICENSE"):
        raise RuntimeError(
            "ANYLOG_LICENSE is not set in the environment. The master's base_configs.env "
            "references $ANYLOG_LICENSE; export it (usually from your shell profile) "
            "before running the harness. See AnyLog-Setup.md section 4."
        )


def _run(cmd: list[str], cwd: str, check: bool = True) -> subprocess.CompletedProcess:
    logger.info(f"$ {' '.join(cmd)}  (cwd={cwd})")
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if proc.returncode != 0:
        msg = f"command failed (rc={proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}"
        if check:
            raise RuntimeError(msg)
        logger.warning(msg)
    return proc


# Operator config generation

_PATCH_KEYS = ("NODE_NAME", "ANYLOG_SERVER_PORT", "ANYLOG_REST_PORT", "DB_PORT", "CLUSTER_NAME")


def _patched_base_config(template_text: str, ident: NodeIdentity) -> str:
    """operator1's base_configs.env with the per-operator values swapped in."""
    values = {
        "NODE_NAME": f'"{ident.operator_container}"',
        "ANYLOG_SERVER_PORT": str(ident.edgelake_tcp_port),
        "ANYLOG_REST_PORT": str(ident.edgelake_rest_port),
        "DB_PORT": str(psql_host_port(ident.index)),
        # one cluster per operator: cluster peers replicate data, benchmarks need shards
        "CLUSTER_NAME": f'"cluster-{ident.operator_container}"',
    }
    lines = []
    for line in template_text.splitlines():
        key = line.split("=", 1)[0].strip()
        if key in _PATCH_KEYS:
            comment = ""
            if "#" in line.split("=", 1)[1]:
                comment = "  #" + line.split("=", 1)[1].split("#", 1)[1]
            line = f"{key}={values[key]}{comment}"
        lines.append(line)
    return "\n".join(lines) + "\n"


def generate_operator_configs(identities: list[NodeIdentity], compose_dir: str) -> None:
    """Create/refresh docker-makefiles/operator<i>-configs/ for every operator from the
    operator1 template. operator1's own dir is regenerated too (idempotent patch), so
    all operators share one source of truth."""
    dm = os.path.join(compose_dir, "docker-makefiles")
    template_base = open(os.path.join(dm, "operator1-configs", "base_configs.env")).read()
    template_advance = os.path.join(dm, "operator1-configs", "advance_configs.env")

    for ident in identities:
        cfg_dir = os.path.join(dm, f"{ident.operator_container}-configs")
        os.makedirs(cfg_dir, exist_ok=True)
        with open(os.path.join(cfg_dir, "base_configs.env"), "w") as f:
            f.write(_patched_base_config(template_base, ident))
        advance_dst = os.path.join(cfg_dir, "advance_configs.env")
        if os.path.abspath(advance_dst) != os.path.abspath(template_advance):
            shutil.copyfile(template_advance, advance_dst)
        logger.info(f"generated AnyLog config dir for {ident.operator_container}")


def _stale_compose_file(node_name: str, compose_dir: str) -> str:
    return os.path.join(
        compose_dir, "docker-makefiles", "docker-compose-files",
        f'"{node_name}"docker-compose.yaml',
    )


def _remove_cached_compose(node_name: str, compose_dir: str) -> None:
    """The Makefile only regenerates a node's compose file if it doesn't exist, so a
    config change (ports, name) would silently keep deploying the old file. Drop the
    cache before `make up`. NODE_NAME is read from base_configs.env quotes-and-all,
    so the cached filename usually carries literal quotes."""
    candidates = [
        _stale_compose_file(node_name, compose_dir),
        os.path.join(compose_dir, "docker-makefiles", "docker-compose-files",
                     f"{node_name}docker-compose.yaml"),
    ]
    for path in candidates:
        if os.path.exists(path):
            os.remove(path)


# Tier orchestration (same interface as infra.py)

def _ensure_node_healthy(
    container: str, rest_conn: str, process_name: str, timeout_s: float = 150.0
) -> None:
    """Deep readiness with one self-heal: the node's deployment script is one-shot and
    sometimes loses a boot-time race (TCP bind, master reachability), leaving a node
    that answers REST but runs no TCP/Operator process. A container restart re-runs
    the script; observed to fix it reliably. Raise if even the restart doesn't."""
    if wait_edgelake_process(rest_conn, process_name, timeout_s=timeout_s):
        return
    logger.warning(
        f"{container}: '{process_name}' process not running after {timeout_s}s; "
        f"restarting the container once (one-shot startup script likely lost a race)"
    )
    subprocess.run(["docker", "restart", container], capture_output=True, text=True)
    if not wait_edgelake_process(rest_conn, process_name, timeout_s=timeout_s):
        raise RuntimeError(
            f"{container} still unhealthy after a restart ('{process_name}' not "
            f"running). Check its logs: docker logs {container} / REST `get error log`."
        )


def infra_up(node_count: int, host, repo_root: str, logical_db: str = "mnist_fl") -> None:
    """Postgres shards -> AnyLog master -> operator config generation -> operators,
    gating each layer on DEEP readiness (processes running, not just REST answering).
    Requires ANYLOG_LICENSE in the environment."""
    require_license()
    compose_dir = _compose_dir(host)
    identities = node_identities(node_count)

    for ident in identities:
        start_postgres(ident.index, repo_root, host)

    _remove_cached_compose("master", compose_dir)
    _run(["make", "up", "ANYLOG_TYPE=master"], cwd=compose_dir, check=True)
    master_rest, _ = master_ports()
    if not wait_edgelake_status(f"{host.EDGELAKE_HOST}:{master_rest}", timeout_s=180):
        raise RuntimeError(
            "AnyLog master never answered `get status`. If it is running but idle, "
            "check the license (expired ANYLOG_LICENSE presents exactly like this)."
        )
    _ensure_node_healthy("master", f"{host.EDGELAKE_HOST}:{master_rest}", "TCP")

    generate_operator_configs(identities, compose_dir)
    for ident in identities:
        _remove_cached_compose(ident.operator_container, compose_dir)
        _run(["make", "up", f"ANYLOG_TYPE={ident.operator_container}"],
             cwd=compose_dir, check=True)

    for ident in identities:
        _ensure_node_healthy(
            ident.operator_container,
            f"{host.EDGELAKE_HOST}:{ident.edgelake_rest_port}",
            "Operator",
        )
    logger.info(f"AnyLog infra up: master + {node_count} operator(s) + {node_count} Postgres shard(s)")


def _remove_node_volumes(node_name: str) -> None:
    """Remove a node's compose volumes regardless of project prefix (compose prefixes
    with the compose-file directory, and the Makefile's own clean-vols misses that)."""
    proc = subprocess.run(["docker", "volume", "ls", "-q"], capture_output=True, text=True)
    pattern = re.compile(rf"(^|_){re.escape(node_name)}-(anylog|blockchain|data|local-scripts|external-lib)$")
    doomed = [v for v in proc.stdout.split() if pattern.search(v)]
    if doomed:
        subprocess.run(["docker", "volume", "rm", "-f", *doomed], capture_output=True, text=True)
        logger.info(f"removed volumes: {doomed}")


def infra_down(node_count: int, host, repo_root: str, remove_volumes: bool = False) -> None:
    compose_dir = _compose_dir(host)
    for ident in reversed(node_identities(node_count)):
        _run(["make", "down", f"ANYLOG_TYPE={ident.operator_container}"],
             cwd=compose_dir, check=False)
        if remove_volumes:
            _remove_node_volumes(ident.operator_container)
    _run(["make", "down", "ANYLOG_TYPE=master"], cwd=compose_dir, check=False)
    if remove_volumes:
        _remove_node_volumes("master")
    for ident in reversed(node_identities(node_count)):
        stop_postgres(ident.index, repo_root, remove_volume=remove_volumes)
    logger.info(f"AnyLog infra down for {node_count} node(s) (volumes removed: {remove_volumes})")


def infra_rebuild(node_count: int, host, repo_root: str, logical_db: str = "mnist_fl",
                  seed_fn=None) -> None:
    infra_down(node_count, host, repo_root, remove_volumes=True)
    infra_up(node_count, host, repo_root, logical_db=logical_db)
    if seed_fn is not None:
        seed_fn()
