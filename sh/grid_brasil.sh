
# Running pipeline per city

declare -a cities=( 'belem' 'belohorizonte' 'brasilia' 'campinas' 'country_brazil' 'curitiba' 'florianopolis' 'fortaleza' 'goiania' 'joaopessoa' 'joinville' 'maceio' 'manaus' 'natal' 'porto_alergre' 'recife' 'riodejaneiro' 'salvador' 'santos' 'saojosedoscampos' 'saoluis' 'saopaulo' 'sorocaba' 'teresina'
'vitoria'  )

for city in ${cities[*]};
do 
    echo $city
    python src/entrypoint.py single --config_path='configs/config-grid.yaml' --dependency_graph_path='configs/dependency-graph-grid.yaml' --slug='allhist' --region_def=$city
done
echo 'All done'


