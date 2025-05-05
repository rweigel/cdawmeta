#!/bin/bash

#cd ../data/; curl -O "https://spdf.gsfc.nasa.gov/skteditor/standalone-skteditor-1.3.11.zip"
# https://spdf.gsfc.nasa.gov/skteditor/src/skteditor-src-1.3.11.zip
# https://github.com/rweigel/skteditor-src
source /Applications/cdf/cdf39_1-dist/bin/definitions.?
export LD_LIBRARY_PATH=$CDF_LIB

#cd ../data; wget --reject "masters.tar" -m --no-parent https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS/
DATA="../data"
# Find all *.cdf files and process them
BASE=$DATA/cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS
JAR=$DATA/skteditor-1.3.11/spdfjavaClasses.jar
CLASS=gsfc.spdf.istp.tools.CDFCheck
#find $BASE -name "a1*.cdf" | while read -r cdf_file; do
find $BASE -name "*.cdf" | while read -r cdf_file; do
  # Run the command and redirect stdout/stderr to a log file
  echo "Processing $cdf_file"

  filename=$(basename "$cdf_file")
  name_part=${filename%%_00000000*}
  name_part_upper=$(echo "$name_part" | tr '[:lower:]' '[:upper:]')
  log_file="$DATA/master/info/${name_part_upper#$BASE/}.errors.skt.log"

  echo "Name part: $name_part_upper"
  echo "Output file: $log_file"
  mkdir -p "$(dirname "$log_file")"
  java -cp $JAR $CLASS "$cdf_file" > "${log_file}" 2>&1
done