cd ~/projects/waze_data_normalization;
conda create --name corona_env -c conda-forge geopandas -y;
sed -i '/h3/d' requirements.txt;
source activate corona_env; pip install -r requirements.txt; pip install h3;
make activate-extensions