git checkout master;
cd ~/projects/waze_coronavirus;
~/.conda/envs/norm_env/bin/python src/entrypoint.py single --slug=prod --force --dependency_graph_path=configs/dependency-graph-prod.yaml --config_path=configs/config-prod.yaml &>> log.log
