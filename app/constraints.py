"""Required placement rules for the simulated scheduler."""

from collections.abc import Mapping

from .models import Cluster, Node, Pod, PodAffinity


def matches(labels: Mapping[str, str], selector: Mapping[str, str]) -> bool:
    return all(labels.get(key) == value for key, value in selector.items())


def peers_in_domain(
    node: Node, peers: Mapping[str, Pod], nodes: Mapping[str, Node], term: PodAffinity,
) -> bool:
    domain = node.labels.get(term.topology_key)
    return domain is not None and any(
        peer.workload in term.workloads
        and nodes[peer.node].labels.get(term.topology_key) == domain
        for peer in peers.values()
    )


def placement_blocks(
    cluster: Cluster, pod: Pod, node: Node, peers: Mapping[str, Pod],
) -> list[str]:
    reasons = []
    if not matches(node.labels, pod.node_selector):
        reasons.append("node_selector")
    if pod.node_affinity and not any(matches(node.labels, term) for term in pod.node_affinity):
        reasons.append("node_affinity")
    if any(
        taint.effect in {"NoSchedule", "NoExecute"}
        and not any(toleration.accepts(taint) for toleration in pod.tolerations)
        for taint in node.taints
    ):
        reasons.append("untolerated_taint")
    for term in pod.pod_affinity:
        if not peers_in_domain(node, peers, cluster.nodes, term):
            reasons.append("pod_affinity")
    for term in pod.pod_anti_affinity:
        if term.topology_key not in node.labels or peers_in_domain(node, peers, cluster.nodes, term):
            reasons.append("pod_anti_affinity")
    for peer in peers.values():
        peer_node = cluster.nodes[peer.node]
        if any(
            pod.workload in term.workloads
            and node.labels.get(term.topology_key) is not None
            and node.labels[term.topology_key] == peer_node.labels.get(term.topology_key)
            for term in peer.pod_anti_affinity
        ):
            reasons.append("existing_pod_anti_affinity")
            break
    return sorted(set(reasons))


def broken_affinities(cluster: Cluster) -> list[str]:
    """Check cluster-wide required co-location after a complete relocation."""
    return sorted(
        pod.name for pod in cluster.pods.values()
        if any(
            not peers_in_domain(
                cluster.nodes[pod.node],
                {name: peer for name, peer in cluster.pods.items() if name != pod.name},
                cluster.nodes,
                term,
            )
            for term in pod.pod_affinity
        )
    )
