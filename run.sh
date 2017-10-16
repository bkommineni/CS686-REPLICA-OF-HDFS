#!/usr/bin/env bash

node_list=(bass02 bass04 bass07 bass08 )
#(bass09 bass11 bass12 bass13 bass14 bass15 bass16 bass17 bass18 bass20 bass21 bass22 bass23) ->working too;just trying on 10 nodes
ControllerHostname=$1
ControllerPort=$2
StorageNodePort=$3

#Killing all the deployed servers
./kill_all.sh

#pull the existing repository for latest changes
git pull origin master

cd retrievedFilesDirectory/

rm -r *

#Controller (bass01)
ssh ${ControllerHostname} "
cd ~/Documents/courses/cs686/p1-bkommineni;
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
	rm -r *;
	cd ~/Documents/courses/cs686/p1-bkommineni;
	java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode $ControllerHostname $ControllerPort $StorageNodePort > storage_${node}.out 2>&1 &
	"
done

#ssh bass24 "
#cd ~/Documents/courses/cs686/p1-bkommineni;
#rm client.out;
#rm -rf data;
#mkdir data;
#java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client $ControllerHostname $ControllerPort list > client.out;
#java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client $ControllerHostname $ControllerPort store ~/Documents/courses/cs686/p1-bkommineni/clientDirectory/File1.txt > client.out 2>&1 &"
