.PHONY = create-env update-env aws-c activate-extensions

SHELL := /bin/bash

REPO=waze_coronavirus

aws-c:
	@source activate norm_env; python ~/private/configs/generate_aws_credentials.py;
	cat ~/private/configs/credentials	
	
create-env: setup-secrets
	conda create --name $(REPO) python=3.7 -y;\
	source activate $(REPO); \
	conda install -c conda-forge h3-py==3.6.4 -y; \
			pip3 install --upgrade -r requirements.txt; \
			python -m ipykernel install --user --name=$(REPO);

update-env:
	source activate $(REPO); \
	pip3 install --upgrade -r requirements.txt;
    
update-env-dev:
	source activate $(REPO); \
	pip3 install --upgrade -r requirements-dev.txt;    

setup-secrets:
	cp ~/shared/spd-sdv-omitnik-waze/corona/configs/* configs/

cron-tab:
	#write out current crontab
	crontab -l > mycron
	#echo new cron into cron file
	echo "0 5 * * *      cd /home/$(USER)/projects/$(REPO); chmod +x run_prod.sh; ./run_prod.sh" >> mycron
	#install new cron file
	crontab mycron
	rm mycron

activate-extensions:
	source activate $(REPO); \
	jupyter contrib nbextension install --user; \
	jupyter nbextension install toc2/main --user; \
	jupyter nbextension enable toc2/main --user; \
	jupyter nbextension install --py --user keplergl; \
	jupyter nbextension enable --py --user keplergl