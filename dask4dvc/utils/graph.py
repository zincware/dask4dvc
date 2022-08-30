import typing
import uuid

import dask.distributed
import networkx as nx


def get_starting_nodes(graph: nx.DiGraph) -> list:
    """Get all nodes from a graph object that do not have a predecessor

    Remove entries containing only "\\n"
    """
    return [
        node for node, in_degree in graph.in_degree if in_degree == 0 and node != "\\n"
    ]


def iterate_over_nodes(
    graph: nx.DiGraph,
) -> typing.List[typing.Tuple[str, typing.List[str]]]:
    """

    Parameters
    ----------
    graph: nx.Graph
        The graph that contains the Nodes

    Attributes
    ----------
    starting_nodes: list[str]
        Name of the Nodes without predecessors / to start from.

    Returns
    ------
    node_name, predecessors: str, list
        The name of the next Node in the graph
        All the immediate predecessors of the given Node

    """

    starting_nodes = get_starting_nodes(graph)

    submitted = []
    node_tuples = []

    for node in starting_nodes:
        node_tuples.append((node, None))
        submitted.append(node)

    for submitted_node in submitted:
        # TODO do not use this function that modifies the list it iterates over.
        #   better use a recursive function
        nodes = graph.successors(submitted_node)
        for node in nodes:
            if node not in submitted:
                node_tuples.append((node, list(graph.predecessors(node))))
                submitted.append(node)
    return node_tuples


def submit_to_dask(
    client: dask.distributed.Client,
    node_pairs: typing.List[tuple],
    cmd: typing.Callable,
    **kwargs,
) -> typing.Dict[str, dask.distributed.Future]:
    """Use the node_pairs to map the DVC graph onto a dask graph.

    Parameters
    ----------
    client: dask.distributed.Client
    node_pairs: list[(name, predecessors)]
        A list of all the nodes to iterate over, starting from the nodes without
        predecessors.
    cmd: Callable
        A function that can be submitted to dask. It MUST take a deps and a name argument.
    kwargs:
        additional kwargs to be passed to the cmd


    Returns
    -------

    dict: A dictionary with a unique submission id as key and the respective dask node
            as value. The unique key is added for parallel execution of experiments with
            the same Node name.

    """

    task_name_map = {}
    for node, deps in node_pairs:
        if deps is not None:
            deps = [task_name_map.get(x) for x in deps]
        if node not in task_name_map:
            # Do not run nodes twice
            dask_node = client.submit(
                cmd,
                name=node,  # required
                deps=deps,  # required
                pure=False,
                **kwargs,
            )
            task_name_map[node] = dask_node

    submission_id = str(uuid.uuid4())[
        :8
    ]  # unique identifier for this graph. This is required when running
    # multiple graphs / experiments in parallel.
    return {f"{submission_id}_{key}": value for key, value in task_name_map.items()}
