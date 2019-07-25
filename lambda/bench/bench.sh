#!/bin/bash

num=$1
benchfile=$2
resultfile=${benchfile%.*}_result.txt
echo -n "" > $resultfile
for i in `seq 1 $num`; do
    output=$(python test.py $benchfile | tail -n 1)
    echo $output
    echo $output >> $resultfile
done
# bashplotlib
hist -f $resultfile -x
