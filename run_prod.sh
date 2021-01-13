git checkout master;
cd ~/projects/IDB-IDB-Invest-Coronavirus-Impact-Dashboard;
~/.conda/envs/IDB-IDB-Invest-Coronavirus-Impact-Dashboard/bin/python src/entrypoint.py single --slug=prod --force --dependency_graph_path=configs/dependency-graph-prod.yaml --config_path=configs/config-prod.yaml &>> log.log
