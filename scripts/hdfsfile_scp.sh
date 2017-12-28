#!/bin/bash

file=$1
dest_ip=$2
destpath=$3
tempdir=$4

echo "starting process for : " $file;
localfileName=$(echo $file | egrep -o "part-.*")
localfileName="$tempdir/$localfileName"
echo "copy to local " $file " to " $localfileName;
hadoop fs -copyToLocal  $file $localfileName;
echo "done copy to local " $file;
scp $localfileName $dest_ip:$destpath
rm $localfileName
echo "--------------------------------------------------";
