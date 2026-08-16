"""
Derived identity: per-node values computed from the node index i (1-based).

Everything here is a function of i, so adding a node means bumping the tier count, not
editing a file. Port scheme matches the README's manual table:

node i  ->  EdgeLake REST = 32{i}49,  TCP = 32{i}48,  uvicorn = 808{i}
 master  ->  i = 0       ->  REST = 32049,        TCP = 32048

So node 1 = 32149/32148/8081, etc. See _edgelake_ports for i >= 10.
"""

from dataclasses import dataclass


# Central CFL aggregator's uvicorn port. Nodes start at 8081.
AGGREGATOR_PORT = 8080


@dataclass(frozen=True)
class NodeIdentity:
    index: int                 # 1-based node number
    replica_name: str          # REPLICA_NAME, e.g. "node1"
    operator_container: str    # EDGELAKE_DOCKER_CONTAINER_NAME, e.g. "operator1"
    edgelake_rest_port: int    # operator REST port
    edgelake_tcp_port: int     # operator TCP port
    uvicorn_port: int          # node_server's own HTTP port


def _edgelake_ports(i: int) -> tuple[int, int]:
    """(rest_port, tcp_port) for index i. Master is i=0 -> (32049, 32048).

    REST = 32000 + i*100 + 49, TCP = 32000 + i*100 + 48.
    """
    rest = 32000 + i * 100 + 49
    tcp = 32000 + i * 100 + 48
    return rest, tcp


def node_identity(i: int) -> NodeIdentity:
    """Build the derived identity for node index i (1-based)."""
    if i < 1:
        raise ValueError(f"node index must be >= 1, got {i}")
    rest, tcp = _edgelake_ports(i)
    return NodeIdentity(
        index=i,
        replica_name=f"node{i}",
        operator_container=f"operator{i}",
        edgelake_rest_port=rest,
        edgelake_tcp_port=tcp,
        uvicorn_port=8080 + i,
    )


def master_ports() -> tuple[int, int]:
    """(rest_port, tcp_port) for the EdgeLake master (i=0)."""
    return _edgelake_ports(0)


def node_identities(node_count: int) -> list[NodeIdentity]:
    """Derived identities for nodes 1..node_count."""
    return [node_identity(i) for i in range(1, node_count + 1)]
