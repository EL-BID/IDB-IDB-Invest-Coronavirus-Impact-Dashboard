SHELL := /bin/bash

start:
	chmod +x start_env.sh
	./start_env.sh
	make activate-extensions

aws-c:
	@source activate norm_env; python ~/private/configs/generate_aws_credentials.py;
	cat ~/private/configs/credentials	

update-env:
	@pip freeze > requirements.txt

activate-extensions:
	@source activate condaenv;  \
	jupyter contrib nbextension install --user; \
	jupyter nbextension install toc2/main --user; \
	jupyter nbextension enable toc2/main --user; \
	jupyter nbextension install --py --user keplergl; \
	jupyter nbextension enable --py --user keplergl
