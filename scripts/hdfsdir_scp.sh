#!/bin/bash

hdfs_path=$1 #'/projects/search/tmp/thejus/cdm-dump/sessions-2017100.split/train'
dest_ip=$2 #'10.89.0.22'
dest_base_path='/mnt/mind_palace/product_ranker/data'
dest_data_path=$3 #"sessions-2017100.split/train"
tempdir=$4 #"/var/lib/semantic/thejus/transfer_2"

destpath="$dest_base_path/$dest_data_path"
echo "creating dest path " $destpath;
ssh $dest_ip "mkdir -p $destpath"

mkdir -p $tempdir

counter=0
for file in $(hadoop fs -ls $hdfs_path | grep part | awk -F" " '{print $8}'); do
    sh hdfsfile_scp.sh $file $dest_ip $destpath $tempdir &
    counter=$((counter + 1))
    if [ $((counter % 10)) -eq 0 ] ;
    then
        wait
    fi
done

rm "part-*"