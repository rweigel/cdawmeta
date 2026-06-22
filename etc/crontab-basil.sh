FILE=data/crontab/basil.$(date +\%Y-\%m-\%dT\%H).log
cd /home/hapi/hapi_meta/cdawmeta
mkdir -p data/crontab
make hapi-update-basil >> $FILE 2>&1
