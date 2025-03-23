FILE=data/crontab/spot10.$(date +\%Y-\%m-\%dT\%H).log
cd ~/cdawmeta/;
mkdir -p data/crontab;
#mkdir -p data/crontab/archive/$(date +\%Y-\%m)
make hapi-update >> $FILE 2>&1
#mv $FILE data/crontab/archive/$(date +\%Y-\%m)

