git checkout master;
cd /home/joaom/projects/waze_coronavirus_prod;
/home/joaom/.conda/envs/norm_env/bin/python src/entrypoint.py single --slug=prod --force &>> log.log
