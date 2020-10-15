
# Running pipeline per city

declare -a cities=( 'belem' 'belohorizonte' 'brasilia' 'campinas' 'country_brazil' 'curitiba' 'florianopolis' 'fortaleza' 'goiania' 'joaopessoa' 'joinville' 'maceio' 'manaus' 'natal' 'porto_alergre' 'recife' 'riodejaneiro' 'salvador' 'santos' 'saojosedoscampos' 'saoluis' 'saopaulo' 'sorocaba' 'teresina'
'vitoria' 'br_states_acre' 'br_states_alagoas' 'br_states_amapa' 'br_states_amazonas' 'br_states_bahia' 'br_states_ceara' 'br_states_distrito_federal' 'br_states_espirito_santo' 'br_states_goias' 'br_states_maranhao' 'br_states_mato_grosso' 'br_states_mato_grosso_do_sul' 'br_states_minas_gerais' 'br_states_para' 'br_states_paraiba' 'br_states_parana' 'br_states_pernambuco' 'br_states_piaui' 'br_states_rio_de_janeiro' 'br_states_rio_grande_do_norte' 'br_states_rio_grande_do_sul' 'br_states_rondonia' 'br_states_roraima' 'br_states_santa_catarina' 'br_states_sao_paulo' 'br_states_sergipe' 'br_states_tocantins' )

for city in ${cities[*]};
do 
    echo $city
    python src/entrypoint.py single --config_path='configs/config-grid.yaml' --dependency_graph_path='configs/dependency-graph-grid.yaml' --slug='allhist' --region_def=$city
done
echo 'All done'


