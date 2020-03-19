import paths
from collections import namedtuple
from copy import deepcopy
from fire import Fire
import logging
import os
from pathlib import Path
import logging
import time
import json
import warnings
import networkx as nx
import pickle as p
import importlib
warnings.filterwarnings('ignore')

from utils import printlog, get_dependency_graph, safe_create_path, get_data_from_athena, to_wkt, get_geometry, generate_query, query_athena, get_config, connect_athena, timed_log
from document import document

def initialize_dag(dependency_graph):
    
    dag = nx.DiGraph()
    
    dag.add_nodes_from([(d['name'], d) for d in dependency_graph])
    
    edges = []
    for d in dependency_graph:
        if 'depends_on' in d.keys():
            for i in d['depends_on']:
                edges.append((i, d['name'],))
                
    dag.add_edges_from(edges)

    p.dump(dag, open('data/output/dependency_graph.p', 'wb'))
    
    return dag

def run_process(_attr, args):

    runner = importlib.import_module(f'runners.{_attr["runner"]}')

    if (not runner.check_existence(_attr)) or (_attr['force']):
        
        exc = ''
        for i in range(_attr['n_tries'] - 1):

            try:
                runner.start(_attr)
                break
            except Exception as e:
                exc = e
                _attr['force'] = True
                print('Retrying')
                continue
        else:
            runner.start(_attr)
            raise Exception(
                    'Number of tries exausted '
                   f'{exc}'
            )

def get_raw_table(attr):

    # skip tables that are not to be processed
    table_type = attr['path'].split('/')[1]

    raw_table = [t for t in attr['tables'] if table_type in t]

    if len(raw_table):

        attr['raw_table'] = raw_table[0] 

    return attr

def core(args):

    args = namedtuple('args', args.keys())(*args.values())

    config = args.config
    config.update(
        dict(verbose=args.verbose, 
            dryrun=args.dryrun,
            runall=args.runall,
            force=args.force,
            n_tries=args.n_tries))

    dependency_graph = get_dependency_graph(args.dependency_graph_path)

    dag = initialize_dag(dependency_graph)

    if args.run_queries:

        for node_name in nx.topological_sort(dag):

            attr = dict(config, **dag.node[node_name])

            attr = get_raw_table(attr)

            if ((attr.get('raw_table') is None) or # skip if table was not selected in config file
                (attr.get('pass') == True)):       # skip if pass == True in dependency graph

                with timed_log(name=attr['name'], config=config, time_chunk='seconds', force='skiped'):
                    continue

            with timed_log(name=attr['name'], config=config, time_chunk='seconds', force=attr['force']):
                run_process(attr, args)

    with timed_log(name='documenting', config=config, time_chunk='seconds', force=args.document):
        if args.document:
            document(args, config)
    

if __name__ == "__main__":

    pass
  

