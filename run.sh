#!/usr/bin/env bash

node_list=(bass02 bass04 bass07 bass08 bass09 bass11 bass12 bass13 bass14 bass15)
#( bass16 bass17 bass18 bass20 bass21 bass22 bass23) ->working too;just trying on 10 nodes
ControllerHostname=$1
ControllerPort=$2
StorageNodePort=$3

#Killing all the deployed servers
./kill_all.sh

#pull the existing repository for latest changes
git pull origin master

#Controller (bass01)
ssh ${ControllerHostname} "
cd ~/Documents/courses/cs686/p1-bkommineni;
rm -rf retrievedFilesDirectory/;
mkdir retrievedFilesDirectory;
/usr/local/maven/bin/mvn compile package;
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
mv dfs-1.0-jar-with-dependencies.jar ~/Documents/courses/cs686/p1-bkommineni/dfs-1.0-jar-with-dependencies.jar;
cd ~/Documents/courses/cs686/p1-bkommineni;
rm controller.out;
rm -r storage_*.out;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller $ControllerPort config/Storage-nodes-list-cluster.txt > controller.out 2>&1 &
"

#StorageNodes

for node in ${node_list[@]};do
	ssh $node "
	cd /home2/bkommineni;
	rm -rf *;
	cd ~/Documents/courses/cs686/p1-bkommineni;
	java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode $ControllerHostname $ControllerPort $StorageNodePort /home2/bkommineni/ > storage_${node}.out 2>&1 &
	"
done