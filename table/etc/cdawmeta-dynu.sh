#!/bin/bash

source /Users/weigel/opt/anaconda3/etc/profile.d/conda.sh; conda activate
conda activate base

cd ../
make serve-cdaweb-sql &
make serve-hapi-sql
