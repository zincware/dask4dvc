import importlib.metadata
import logging

import dask.distributed

import dask4dvc.dvc_handling
import dask4dvc.utils

log = logging.getLogger(__name__)

__version__ = importlib.metadata.version("dask4dvc")


def submit_to_dask(client: dask.distributed.Client, nodes):
    task_name_map = {}
    for node, deps in nodes:
        if deps is not None:
            deps = [task_name_map.get(x) for x in deps]
        if node not in task_name_map:
            # Do not run nodes twice
            dask_node = client.submit(
                dask4dvc.dvc_handling.submit_dvc_stage,
                node_name=node,
                deps=deps,
                pure=False,
            )
            task_name_map[node] = dask_node

    return task_name_map


def run():
    # Check git status and raise error=!
    graph = dask4dvc.dvc_handling.get_dvc_graph()
    starting_nodes = dask4dvc.utils.get_starting_nodes(graph)
    nodes = list(dask4dvc.utils.iterate_over_nodes(graph, starting_nodes=starting_nodes))
    client = dask.distributed.Client()
    log.warning("Dask server initialized")
    output = submit_to_dask(client, nodes)

    dask4dvc.utils.wait_for_futures(output)
    log.warning("Loading into workspace")
    result = client.submit(
        dask4dvc.dvc_handling.run_dvc_repro_in_cwd,
        node_name=None,
        deps=output,
        pure=False,
    )
    _ = result.result()  # this should only run if all outputs are gathered successfully

    _ = input("Press Enter to close the client")

    client.close()


if __name__ == "__main__":
    run()
