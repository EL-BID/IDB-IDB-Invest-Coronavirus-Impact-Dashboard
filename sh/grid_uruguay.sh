
# Running pipeline per city

declare -a cities=('montevideo')

for city in ${cities[*]};
do 
    echo $city
    python src/entrypoint.py single --config_path='configs/config-grid.yaml' --dependency_graph_path='configs/dependency-graph-grid.yaml' --slug='allhist' --region_def=$city
done
echo 'All done'

