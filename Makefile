PYTHON=~/anaconda3/bin/python

#ID_SKIP=--id-skip '^PSP'
ID_SKIP=
NO_UPDATE=cadence
NO_REGEN=$(NO_UPDATE)
UPDATE=$(ID_SKIP) --write-catalog --update --update-skip $(NO_UPDATE) --max-workers 1
REGEN=$(ID_SKIP) --write-catalog  --regen --regen-skip $(NO_REGEN) --max-workers 1

spase_auto-update: cdawmeta.egg-info
	python metadata.py --meta-type spase_auto $(UPDATE)

spase_auto-regen: cdawmeta.egg-info
	python metadata.py --meta-type hapi $(REGEN)

hapi-update: cdawmeta.egg-info
	python metadata.py --meta-type hapi $(UPDATE)

hapi-updatex: cdawmeta.egg-info
	python metadata.py --meta-type hapi $(UPDATE) --id-skip '^MMS|^C|^T'
	python metadata.py --meta-type hapi $(UPDATE) --id '^MMS|^C|^T'

hapi-regen: cdawmeta.egg-info
	python metadata.py --meta-type hapi $(REGEN)

all-regen: cdawmeta.egg-info
	python metadata.py $(REGEN)

all-update: cdawmeta.egg-info
	python metadata.py $(UPDATE)

cadence-regen: cdawmeta.egg-info
	python metadata.py --id-skip '$(ID_SKIP)' --meta-type cadence --regen --write-catalog

clean:
	-rm -rf data/*

test-README: cdawmeta.egg-info
	python metadata.py --id AC_OR_SSC --meta-type hapi
	python metadata.py --id AC_OR_SSC --meta-type hapi --update
	python metadata.py --id AC_OR_SSC --meta-type hapi --regen
	python metadata.py --id AC_OR_SSC --meta-type spase_auto
	python metadata.py --id VOYAGER1_10S_MAG --meta-type AccessInformation
	python metadata.py --id VOYAGER1_10S_MAG --meta-type start_stop
	python metadata.py --id VOYAGER1_10S_MAG --meta-type cadence
	python metadata.py --id VOYAGER1_10S_MAG --meta-type sample_links

test-table: cdawmeta.egg-info
	python table.py --id '^AC_OR'

test-report: cdawmeta.egg-info
	python report.py --id AC_OR_DEF --update

rsync-to-spot10:
	rsync -avz -e 'ssh -p 890' \
		--delete \
		--exclude data \
		--no-links \
		--delete \
		../cdawmeta weigel@cottagesystems.com:
	rsync -avz -e 'ssh -p 890' \
		--delete \
		--exclude data \
		--no-links \
		--delete \
		data/hapi weigel@cottagesystems.com:cdawmeta/data
	rsync -avz -e 'ssh -p 890' \
		--delete \
		--exclude data \
		--no-links \
		--delete \
		data/orig_data weigel@cottagesystems.com:cdawmeta/data

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

skterrors:
	find data/cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS -name "*.cdf" | xargs -J{} java -cp data/skteditor-1.3.11/spdfjavaClasses.jar gsfc.spdf.istp.tools.CDFCheck {} > {}.log

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
	conda create --name $(CENV) pip -y
	conda activate $(CENV)
	conda install pip
