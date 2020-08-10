import paths
from fire import Fire
import logging
from pathlib import Path
import networkx as nx
import pickle as p
import importlib
import warnings
from loguru import logger as loguru

warnings.filterwarnings("ignore")

from utils import get_dependency_graph, safe_create_path, timed_log


def make_dag(dependency_graph):

    dag = nx.DiGraph()

    dag.add_nodes_from([(d["name"], d) for d in dependency_graph])

    edges = []
    for d in dependency_graph:
        if "depends_on" in d.keys():
            for i in d["depends_on"]:
                if i in [d["name"] for d in dependency_graph]:
                    edges.append((i, d["name"]))

    dag.add_edges_from(edges)

    safe_create_path("data/output")
    p.dump(dag, open("data/output/dependency_graph.p", "wb"))

    return dag


def initialize_dag(config):

    dependency_graph = get_dependency_graph(config["dependency_graph_path"])

    G = make_dag(dependency_graph)

    if not config["force"]:

        sources = [d["name"] for d in dependency_graph if d.get("force")]

        flatten = lambda l: [item for sublist in l for item in sublist]
        nodes_to_run = set(
            flatten([[d for d in nx.dfs_preorder_nodes(G, source=s)] for s in sources])
        )

        if config["force_downstream"]:

            dependency_graph = [
                dict({"force": True}, **d)
                for d in dependency_graph
                if d["name"] in nodes_to_run
            ]
        else:

            dependency_graph = [
                d for d in dependency_graph if d["name"] in nodes_to_run
            ]

        G = make_dag(dependency_graph)

    return [G.node[name] for name in nx.topological_sort(G)]


def run_process(_attr):

    runner = importlib.import_module(f'runners.{_attr["runner"]}')

    name = _attr["name"]

    if (not runner.check_existence(_attr)) or (_attr["force"]):

        exc = ""
        for i in range(_attr["n_tries"] - 1):

            try:
                runner.start(_attr)
                break
            except Exception as e:
                exc = e
                _attr["force"] = True
                _attr["name"] = name
                print("Retrying")
                continue
        else:
            runner.start(_attr)


def get_raw_table(attr):

    # skip tables that are not to be processed
    table_type = attr["path"].split("/")[1]

    raw_table = [t for t in attr["tables"] if table_type in t]

    if len(raw_table):

        attr["raw_table"] = raw_table[0]

    return attr


@loguru.catch
def core(config):

    for attr in initialize_dag(config):

        attr = dict(config, **attr)

        attr = get_raw_table(attr)

        if (
            attr.get("raw_table") is None
        ) or (  # skip if table was not selected in config file
            attr.get("pass") == True
        ):  # skip if pass == True in dependency graph

            with timed_log(
                name=attr["name"], config=config, time_chunk="seconds", force="skiped"
            ):
                continue

        with timed_log(
            name=attr["name"], config=config, time_chunk="seconds", force=attr["force"]
        ):
            run_process(attr)


if __name__ == "__main__":

    pass

