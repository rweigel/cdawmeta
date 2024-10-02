#!/bin/bash

source ~/anaconda3/etc/profile.d/conda.sh; conda activate
conda activate python3.10.9-cdawmeta

pkill -f "python table/table-ui/serve.py"
cd ../..
python table/table-ui/serve.py --port 8051 --sqldb data/table/cdaweb.variable.sql &
python table/table-ui/serve.py --port 8052 --sqldb data/table/cdaweb.dataset.sql &
python table/table-ui/serve.py --port 8053 --sqldb data/table/spase.parameter.sql &
python table/table-ui/serve.py --port 8054 --sqldb data/table/spase.dataset.sql
