from fire import Fire
from pathlib import Path

from core import core
from utils import timed_log, get_config
import logger

class Run(object):

    def __init__(self,
        slug='dev',
        verbose=False,
        dryrun=False,
        force_downstream=False,
        runall=False,
        force=False,
        n_tries=5,
        run_queries=True,
        dependency_graph_path='configs/dependency_graph.yaml'):
        """Entrypoint function. 
        """

        self.verbose = verbose 
        self.dryrun = dryrun 
        self.runall = runall 
        self.force_downstream = force_downstream
        self.force = force
        self.dependency_graph_path = dependency_graph_path
        self.n_tries = n_tries
        self.slug = slug

        self.args = vars(self)

    def single(self, config_path='configs/config-template.yaml'):
        """Run the pipeline for a single region
        
        Parameters
        ----------
        config_path : str, optional
            config.yaml path, by default 'configs/config.yaml'
        """

        config = get_config(config_path)
        config.update(self.args)

        if self.verbose: 
            print(config)

        with timed_log(name='Start process', config=config, time_chunk='minutes', 
                        force=config['force']):
                pass
                
        with timed_log(name='Full process', config=config, time_chunk='minutes', 
                        force=config['force']):

            if config['slug'] == 'prod':
                try:
                    core(config)
                except Exception as e:
                    logger.post(e)
                    print(e)
            else:
                core(config)

if __name__ == "__main__":

    Fire(Run)