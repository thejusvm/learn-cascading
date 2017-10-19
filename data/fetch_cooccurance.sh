#/bin/bash

pid=$1
num=$2
echo "======================poitive"
cat sessionExplode-201708.MOB/part-* | grep -i "search" | awk -F'\t' '$7 ~ /'$pid'/ {print $5" "$7}' | cut -d" " -f 1 | sort | uniq -c | sort -n -r -k 1 | head -n $num
echo "======================negative"
cat sessionExplode-201708.MOB/part-* | grep -i "search" | awk '$7 ~ /'$pid'/ {print $0}'  | awk -F'\t' '{print $6" "$7}' | cut -d" " -f 1 | egrep -o '"[^"]+"' | sed "s|\"||g"| sort | uniq -c | sort -n -r -k 1 | head -n $num

