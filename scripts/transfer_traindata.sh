#!/bin/bash


hdfs_path=$1 #'/projects/search/tmp/thejus/cdm-dump/sessions-2017100.split
dest_ip=$2
dest_data_path=$3 #"sessions-2017100.split"
tempdir=$4 #"/var/lib/semantic/thejus/transfer_2"

sh hdfsdir_scp.sh "$hdfs_path/train" $dest_ip "$dest_data_path/train" "$tempdir/train"
sh hdfsdir_scp.sh "$hdfs_path/test" $dest_ip "$dest_data_path/test" "$tempdir/test"