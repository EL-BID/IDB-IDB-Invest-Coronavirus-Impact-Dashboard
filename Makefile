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

setup-secrets:
	cp ~/shared/spd-sdv-omitnik-waze/corona/configs/* configs/


chron-tab:
	echo "0,10,20,30,40,50 * * * *  /usr/local/stata16/cloudsync.sh" >> /usr/local/stata16/cloudsync.cron; \
	service cron reload; 
	