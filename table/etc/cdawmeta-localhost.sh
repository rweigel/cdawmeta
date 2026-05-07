rsync -avz ../../data/table weigel@rweigel.dynu.net:git/hapi/cdawmeta/data/

source /opt/miniconda3/etc/profile.d/conda.sh; conda activate
conda activate python3.10.9-cdawmeta

pkill -f "python serve.py --port 8051"
tableui-serve --port 8051 --config ../conf/cdaweb.json &

sleep 1 # Wait for server to start
# Open browser to test
open http://localhost:8051