import pathlib
import typing
import uuid

import networkx as nx
from dask.distributed import Future


def get_starting_nodes(graph: nx.DiGraph) -> list:
    """Get all nodes from a graph object that do not have a predecessor

    Remove entries containing only "\\n"
    """
    return [
        node for node, in_degree in graph.in_degree if in_degree == 0 and node != "\\n"
    ]


def get_tmp_dir(uuid_length: int = 8) -> pathlib.Path:
    """Get a random path"""
    return pathlib.Path(f"tmp_{str(uuid.uuid4())[:uuid_length]}")


def iterate_over_nodes(
    graph: nx.DiGraph, starting_nodes: list
) -> typing.Generator[typing.Tuple[str, typing.List[str]], None, None]:
    """

    Parameters
    ----------
    graph: nx.Graph
        The graph that contains the Nodes
    starting_nodes: list[str]
        Name of the Nodes without predecessors / to start from.

    Yields
    ------
    node_name, predecessors: str, list
        The name of the next Node in the graph
        All the immediate predecessors of the given Node

    """
    submitted = []

    for node in starting_nodes:
        yield node, None
        submitted.append(node)

    for submitted_node in submitted:
        # TODO do not use this function that modifies the list it iterates over.
        #   better use a recursive function
        nodes = graph.successors(submitted_node)
        for node in nodes:
            if node not in submitted:
                yield node, list(graph.predecessors(node))
                submitted.append(node)


def wait_for_futures(futures: typing.Dict[str, Future]):
    for future in futures.values():
        _ = future.result()
