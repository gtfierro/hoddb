#!/bin/bash

set -ex

go build
for ttl in `ls mortarsites`; do
    rm -rf _hod_
    building="${ttl%.*}"
    echo $building
    ./make_backup -config hodconfig.yml -ttl mortarsites/$ttl -building $building
    mv $building.badger graphs
done
