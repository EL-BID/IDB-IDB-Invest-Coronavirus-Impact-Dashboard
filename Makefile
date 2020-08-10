.PHONY = create-env update-env aws-c activate-extensions

SHELL := /bin/bash

REPO=$(shell basename $(CURDIR))

aws-c:
	@source activate norm_env; python ~/private/configs/generate_aws_credentials.py;
	cat ~/private/configs/credentials	

create-env:
	conda create --name $(REPO) -c conda-forge h3-py -y;
	source activate $(REPO); \
			pip3 install --upgrade -r requirements.txt; \
			python -m ipykernel install --user --name=$(REPO);

update-env:
	source activate $(REPO); \
	pip3 install --upgrade -r requirements.txt;