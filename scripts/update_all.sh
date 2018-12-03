#!/bin/bash

for val in 0{1..9} 10
do
    echo VM$val
    ssh kechenl3@fa18-cs425-g29-$val.cs.illinois.edu "cd ~/go/src/crane_pure; git reset --hard; git pull; cd ./core/driver ; go build; cd ../supervisor/; go build; cd ../../tools/sdfs_client/;go build; exit"
done
echo 'Git Update!'

