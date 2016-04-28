#!/bin/bash

set -e
tmp_container="heka_instance"
release_img="heka_release"

docker build -t $release_img .
docker create --name $tmp_container $release_img
docker cp $tmp_container:/heka_build/heka-lain-0.10.tgz ./heka-lain.tgz
docker rm -f $tmp_container
docker rmi -f $release_img
