#!/bin/bash

source ~/anaconda3/etc/profile.d/conda.sh; conda activate
conda activate python3.10.9-cdawmeta

pkill -f "python serve.py --port 8051"
tableui-serve --port 8051 --config ../conf/cdaweb.json
