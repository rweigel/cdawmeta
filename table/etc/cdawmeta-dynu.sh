#!/bin/bash

source ~/anaconda3/etc/profile.d/conda.sh; conda activate
conda activate python3.10.9-cdawmeta

pkill -f "python serve.py --port 8051"
cd ../../../table-ui;
python serve.py --port 8051 --config ../cdawmeta/table/conf/cdaweb.json
