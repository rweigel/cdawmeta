#!/bin/bash

source /Users/weigel/miniconda3/etc/profile.d/conda.sh; conda activate
conda activate base

cd ../; make serve-cdaweb-sql
