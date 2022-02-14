git checkout master;
cd ~/projects/IDB-IDB-Invest-Coronavirus-Impact-Dashboard;
~/.conda/envs/IDB-IDB-Invest-Coronavirus-Impact-Dashboard/bin/python src/entrypoint.py single --slug=dev --force --dependency_graph_path=configs/dependency-graph-dev.yaml --config_path=configs/config-dev.yaml &>> log-dev.log