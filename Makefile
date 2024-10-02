PYTHON=~/anaconda3/bin/python

hapi-update: cdawmeta.egg-info
	python metadata.py --meta-type hapi --write-catalog --update --update-skip cadence

hapi-regen: cdawmeta.egg-info
	python metadata.py --meta-type hapi --id-skip '^PSP' --write-catalog --regen --regen-skip cadence --max-workers 1

regen-all: cdawmeta.egg-info
	python metadata.py --meta-type hapi --regen --regen-skip cadence --write-catalog

update-all: cdawmeta.egg-info
	python metadata.py --meta-type hapi --update --update-skip cadence --write-catalog

cadence-regen: cdawmeta.egg-info
	python metadata.py --id-skip '^PSP' --meta-type cadence --regen --write-catalog

clean:
	-rm -rf data/*

test-README: cdawmeta.egg-info
	python metadata.py --id AC_OR_SSC --meta-type hapi
	python metadata.py --id AC_OR_SSC --meta-type spase_auto
	python metadata.py --id AC_OR_SSC --meta-type soso
	python metadata.py --id AC_OR_SSC --update hapi
	python metadata.py --id AC_OR_SSC --regen hapi

test-table: cdawmeta.egg-info
	python table.py --id '^AC_OR'

test-report: cdawmeta.egg-info
	python report.py --id AC_OR_DEF --update

rsync-to-mag:
	rsync -avz \
		--exclude data/hpde.io --exclude data/cdaweb.gsfc.nasa.gov \
		--no-links \
		--delete \
		data weigel@mag.gmu.edu:www/git-data/cdawmeta

rsync-from-mag:
	rsync -avz weigel@mag.gmu.edu:www/git-data/cdawmeta/ .

cdawmeta.egg-info:
	pip install -e .
################################################################################

################################################################################
table/table-ui:
	@- cd table; git clone https://github.com/rweigel/table-ui
	@- cd table; git pull https://github.com/rweigel/table-ui

# Generate all tables
table-update: cdawmeta.egg-info table/table-ui
	python table.py --update

# Use code to generate table code or metadata code it uses changes
table-regen: cdawmeta.egg-info
	python table.py

data/table/cdaweb.variable.sql: cdawmeta.egg-info
	python table.py --table-name cdaweb.variable

data/table/cdaweb.dataset.sql: cdawmeta.egg-info
	python table.py --table-name cdaweb.dataset

data/table/spase.dataset.sql: cdawmeta.egg-info
	python table.py --table-name spase.dataset

data/table/spase.parameter.sql: cdawmeta.egg-info
	python table.py --table-name spase.parameter

table-serve: #cdawmeta.egg-info data/table/cdaweb.variable.sql data/table/cdaweb.dataset.sql data/table/spase.parameter.sql data/table/spase.dataset.sql
	-pkill -f "python table/table-ui/serve.py"
	$(PYTHON) table/table-ui/serve.py --port 8051 --sqldb data/table/cdaweb.variable.sql &
	$(PYTHON) table/table-ui/serve.py --port 8052 --sqldb data/table/cdaweb.dataset.sql &
	$(PYTHON) table/table-ui/serve.py --port 8053 --sqldb data/table/spase.parameter.sql &
	$(PYTHON) table/table-ui/serve.py --port 8054 --sqldb data/table/spase.dataset.sql
################################################################################

# Not finished
CENV=python3.10.9-cdawmeta-test
conda-env:
	- echo "$$CONDA_DEFAULT_ENV" | grep -q "^$(CENV)" && conda deactivate 2> /dev/null || true
	- conda env list | grep -q "^$(CENV)" && conda remove --name $(CENV) --all -y 2> /dev/null || true
	conda create --name $(CENV) -y
