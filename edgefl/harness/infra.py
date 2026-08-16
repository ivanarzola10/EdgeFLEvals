"""Docker containers: Postgres shards, EdgeLake master and operators.

Unlike the Python servers in process.py, these are Docker containers driven through the
existing Makefiles. This module wraps those targets plus the EdgeLake operator env-file
generation needed to scale past the committed operator1-4.

Startup ordering matters, since operators need the master's IP:
  1. Postgres shards
  2. EdgeLake master
  3. discover master docker IP for the operators' LEDGER_CONN
  4. generate operatorN.env + edgelake_operatorN.env
  5. EdgeLake operators

Seams marked runtime-validation-needed depend on a live Docker daemon and the EdgeLake
image, so they can't be exercised without them.
"""

import subprocess

from platform_components.lib.logger.error_handling import get_logger
from .identity import NodeIdentity, _edgelake_ports, master_ports, node_identities
from .readiness import wait_edgelake_status

logger = get_logger(__name__)


DEFAULT_TAG = "1.3.2501-arm64"
PSQL_BASE_PORT = 5432  # postgres1 -> 5432, postgres2 -> 5433, ...


def _run(cmd: list[str], cwd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a subprocess, capturing output and logging the command. Raises on failure when
    check=True; callers that tolerate 'already exists' pass check=False."""
    logger.info(f"$ {' '.join(cmd)}  (cwd={cwd})")
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if proc.returncode != 0:
        msg = f"command failed (rc={proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}"
        if check:
            raise RuntimeError(msg)
        logger.warning(msg)
    return proc


# Postgres

def psql_host_port(i: int) -> int:
    """Host port for the i-th Postgres shard (1-based). postgres1 -> 5432."""
    return PSQL_BASE_PORT + (i - 1)


def _container_exists(name: str) -> bool:
    proc = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name=^/{name}$", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    return proc.stdout.strip() == name


def start_postgres(i: int, edgelake_dir: str, host) -> None:
    """Start one Postgres shard via EdgeLake/postgres/Makefile.

    The Makefile's `up` target refuses to run if a container with that name already
    exists (exits 1: "already exists. Use 'make restart' or 'make down' first") —
    exactly the state left behind by a prior teardown that didn't remove volumes.
    Detect that case and `docker start` the existing container instead of leaving it
    stopped (operators depend on their shard actually running)."""
    pg_dir = f"{edgelake_dir}/EdgeLake/postgres"
    name = f"postgres{i}"
    if _container_exists(name):
        proc = subprocess.run(["docker", "start", name], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"could not start existing container {name}: {proc.stderr.strip()}")
        logger.info(f"{name} already existed (stopped); started it")
        return
    _run(
        ["make", "up", f"NAME={name}", f"HOST_PORT={psql_host_port(i)}",
         f"VOLUME=pgdata{i}", f"POSTGRES_USER={host.PSQL_USER}",
         f"POSTGRES_PASSWORD={host.PSQL_PASSWORD}"],
        cwd=pg_dir, check=True,
    )


def stop_postgres(i: int, edgelake_dir: str, remove_volume: bool = False) -> None:
    pg_dir = f"{edgelake_dir}/EdgeLake/postgres"
    target = "clean" if remove_volume else "down"
    _run(["make", target, f"NAME=postgres{i}", f"VOLUME=pgdata{i}"],
         cwd=pg_dir, check=False)


# EdgeLake containers

def start_master(edgelake_dir: str, tag: str = DEFAULT_TAG) -> None:
    """Start the EdgeLake master (the shared blockchain ledger)."""
    rest, tcp = master_ports()
    _run(
        ["make", "up", "EDGELAKE_TYPE=master", f"TAG={tag}",
         f"EDGELAKE_SERVER_PORT={tcp}", f"EDGELAKE_REST_PORT={rest}",
         "NODE_NAME=master"],
        cwd=f"{edgelake_dir}/EdgeLake", check=True,
    )


def discover_master_ip(container_name: str = "master") -> str:
    """Resolve the master container's docker-bridge IP, used as LEDGER_CONN for operators
    (mirrors the README's `docker inspect` step). Runtime-validation-needed."""
    proc = _run(
        ["docker", "inspect", "-f",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container_name],
        cwd=".", check=True,
    )
    ip = proc.stdout.strip()
    if not ip:
        raise RuntimeError(f"could not resolve docker IP for container '{container_name}'")
    logger.info(f"master docker IP = {ip}")
    return ip


def start_operator(identity: NodeIdentity, edgelake_dir: str, tag: str = DEFAULT_TAG) -> None:
    """Start one EdgeLake operator container. Assumes its env files (operatorN.env +
    edgelake_operatorN.env) were already generated."""
    _run(
        ["make", "up", "EDGELAKE_TYPE=operator", f"TAG={tag}",
         f"EDGELAKE_SERVER_PORT={identity.edgelake_tcp_port}",
         f"EDGELAKE_REST_PORT={identity.edgelake_rest_port}",
         f"NODE_NAME={identity.operator_container}"],
        cwd=f"{edgelake_dir}/EdgeLake", check=True,
    )


def stop_edgelake(node_name: str, edgelake_dir: str, tag: str = DEFAULT_TAG,
                  remove_volumes: bool = False) -> None:
    # The compose template substitutes the port vars even on `down`; leaving them
    # empty yields an invalid compose file and the down silently no-ops. Ports are
    # formulaic per node name ("master" or "operator<i>").
    if node_name == "master":
        rest, tcp = master_ports()
    else:
        rest, tcp = _edgelake_ports(int(node_name.removeprefix("operator")))
    _run(["make", "down", f"EDGELAKE_TYPE={'master' if node_name=='master' else 'operator'}",
          f"TAG={tag}", f"NODE_NAME={node_name}",
          f"EDGELAKE_SERVER_PORT={tcp}", f"EDGELAKE_REST_PORT={rest}"],
         cwd=f"{edgelake_dir}/EdgeLake", check=False)
    if remove_volumes:
        # Not `make clean`: its `down -v --rmi all` also deletes the EdgeLake image,
        # forcing a re-pull on every full teardown. Compose prefixes volumes with the
        # compose-file directory name.
        vols = [
            f"docker_makefile_{node_name}-{suffix}"
            for suffix in ("anylog", "blockchain", "data", "local-scripts")
        ]
        _run(["docker", "volume", "rm", "-f", *vols], cwd=".", check=False)


# EdgeLake operator env-file generation

def generate_operator_env_files(
    identities: list[NodeIdentity],
    edgelake_dir: str,
    master_ip: str,
    logical_db: str,
    host,
    tag: str = DEFAULT_TAG,
) -> None:
    """Generate the operator env pair for each operator beyond the committed operator1-4,
    into EdgeLake/docker_makefile/ where the Makefile expects them: operatorN.env (compose
    vars) and edgelake_operatorN.env (full config with LEDGER_CONN, DB_*). Modeled on the
    committed operator1 files; LEDGER_CONN uses the live master IP.
    """
    dm = f"{edgelake_dir}/EdgeLake/docker_makefile"
    _, tcp_master = master_ports()

    for ident in identities:
        name = ident.operator_container
        # compose-template env
        compose_env = (
            f"EDGELAKE_TYPE=operator\n"
            f"TAG={tag}\n"
            f"EDGELAKE_SERVER_PORT={ident.edgelake_tcp_port}\n"
            f"EDGELAKE_REST_PORT={ident.edgelake_rest_port}\n"
            f"NODE_NAME={name}\n"
            f'BLOCKCHAIN_SYNC="10 seconds"\n'
        )
        _write(f"{dm}/{name}.env", compose_env)

        # full operator config, modeled on edgelake_operator1.env. DB_IP must be
        # reachable FROM INSIDE the operator container: 127.0.0.1 there is the
        # container itself, so Postgres is addressed through the Docker-internal
        # host alias and its published host port.
        full_env = _render_edgelake_operator_config(
            name=name,
            tcp_port=ident.edgelake_tcp_port,
            rest_port=ident.edgelake_rest_port,
            db_ip=getattr(host, "DOCKER_INTERNAL_HOST", "host.docker.internal"),
            db_port=psql_host_port(ident.index),
            db_user=host.PSQL_USER,
            db_passwd=host.PSQL_PASSWORD,
            ledger_conn=f"{master_ip}:{tcp_master}",
            logical_db=logical_db,
        )
        _write(f"{dm}/edgelake_{name}.env", full_env)
        logger.info(f"generated EdgeLake env for {name} (LEDGER_CONN={master_ip}:{tcp_master})")


def _render_edgelake_operator_config(
    *, name, tcp_port, rest_port, db_ip, db_port, db_user, db_passwd,
    ledger_conn, logical_db,
) -> str:
    """The full edgelake_operatorN.env body, trimmed to the fields that vary or matter for
    FL. Mirrors the committed operator1 file."""
    return (
        f"NODE_TYPE=operator\n"
        f"NODE_NAME={name}\n"
        f"COMPANY_NAME=New Company\n"
        f"ANYLOG_SERVER_PORT={tcp_port}\n"
        f"ANYLOG_REST_PORT={rest_port}\n"
        f'ANYLOG_BROKER_PORT=""\n'
        f"TCP_BIND=true\n"
        f"REST_BIND=false\n"
        f"BROKER_BIND=false\n"
        f"DB_TYPE=psql\n"
        f'DB_USER="{db_user}"\n'
        f'DB_PASSWD="{db_passwd}"\n'
        f"DB_IP={db_ip}\n"
        f"DB_PORT={db_port}\n"
        f"AUTOCOMMIT=false\n"
        f"SYSTEM_QUERY=true\n"
        f"MEMORY=true\n"
        f"LEDGER_CONN={ledger_conn}\n"
        f"BLOCKCHAIN_SYNC=10 seconds\n"
        f"CLUSTER_NAME=new-cluster\n"
        f"DEFAULT_DBMS={logical_db}\n"
        f"ENABLE_PARTITIONS=true\n"
        f"PARTITION_COLUMN=insert_timestamp\n"
        f"PARTITION_INTERVAL=14 days\n"
        f"PARTITION_KEEP=3\n"
        f"PARTITION_SYNC=1 day\n"
        f"ENABLE_MQTT=false\n"
        f"MONITOR_NODES=true\n"
        f"STORE_MONITORING=false\n"
        f"SYSLOG_MONITORING=false\n"
        f"DEPLOY_LOCAL_SCRIPT=false\n"
    )


def _write(path: str, text: str) -> None:
    with open(path, "w") as f:
        f.write(text)


# Tier orchestration: everything Docker for N nodes, in dependency order.

def infra_up(
    node_count: int,
    host,
    repo_root: str,
    logical_db: str = "mnist_fl",
    tag: str = DEFAULT_TAG,
) -> None:
    """Bring up the full Docker stack for a tier: Postgres shards -> master ->
    operator env generation (needs the live master IP) -> operators, gating each layer
    on readiness. Idempotent-ish: `make up` tolerates already-running containers.

    This regenerates EdgeLake/docker_makefile/operatorN.env for ALL N operators —
    including the committed operator1-4 files — because LEDGER_CONN must carry the live
    master's docker IP. Expect those four files to show as modified in git.
    """
    identities = node_identities(node_count)

    for ident in identities:
        start_postgres(ident.index, repo_root, host)

    start_master(repo_root, tag=tag)
    master_rest, _ = master_ports()
    if not wait_edgelake_status(f"{host.EDGELAKE_HOST}:{master_rest}", timeout_s=180):
        raise RuntimeError("EdgeLake master never answered `get status`")

    master_ip = discover_master_ip()
    generate_operator_env_files(identities, repo_root, master_ip, logical_db, host, tag=tag)

    for ident in identities:
        start_operator(ident, repo_root, tag=tag)

    not_ready = [
        f"{host.EDGELAKE_HOST}:{i.edgelake_rest_port}"
        for i in identities
        if not wait_edgelake_status(f"{host.EDGELAKE_HOST}:{i.edgelake_rest_port}", timeout_s=180)
    ]
    if not_ready:
        raise RuntimeError(f"operators never became ready: {not_ready}")
    logger.info(f"infra up: master + {node_count} operator(s) + {node_count} Postgres shard(s)")


def infra_down(
    node_count: int,
    host,
    repo_root: str,
    remove_volumes: bool = False,
    tag: str = DEFAULT_TAG,
) -> None:
    """Tear down the tier's Docker stack, reverse order. remove_volumes=True is the
    Full-teardown variant: blockchain and Postgres data are gone, so reseed after."""
    for ident in reversed(node_identities(node_count)):
        stop_edgelake(ident.operator_container, repo_root, tag=tag, remove_volumes=remove_volumes)
    stop_edgelake("master", repo_root, tag=tag, remove_volumes=remove_volumes)
    for ident in reversed(node_identities(node_count)):
        stop_postgres(ident.index, repo_root, remove_volume=remove_volumes)
    logger.info(f"infra down for {node_count} node(s) (volumes removed: {remove_volumes})")


def infra_rebuild(
    node_count: int,
    host,
    repo_root: str,
    logical_db: str = "mnist_fl",
    tag: str = DEFAULT_TAG,
    seed_fn=None,
) -> None:
    """Full rebuild: down with volumes, up fresh, reseed. This is the forced-Full path
    for tier crossings and mid-run failures; seed_fn re-inserts the dataset."""
    infra_down(node_count, host, repo_root, remove_volumes=True, tag=tag)
    infra_up(node_count, host, repo_root, logical_db=logical_db, tag=tag)
    if seed_fn is not None:
        seed_fn()
