#!/usr/bin/env bash

node_list=(bass02 bass03 bass04 bass05 bass06 bass07 bass08 bass09 bass10 bass11 bass12 bass13 bass14 bass15 bass16 bass17 bass18 bass19 bass20 bass21 bass22 bass23)
port=$1

#Killing all the deployed servers
./kill_all.sh

#pull the existing repository for latest changes
git pull origin master

#Controller (bass01)
ssh bass01 "
cd ~/Documents/courses/cs686/p1-bkommineni;
/usr/local/maven/bin/mvn compile package;
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
mv dfs-1.0-jar-with-dependencies.jar ~/Documents/courses/cs686/p1-bkommineni/dfs-1.0-jar-with-dependencies.jar;
cd ~/Documents/courses/cs686/p1-bkommineni;
rm controller.out;
rm -rf data;
mkdir data;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller $port > controller.out 2>&1 &
"

#StorageNodes

for node in ${node_list[@]};do
	ssh $node "
	cd ~/Documents/courses/cs686/p1-bkommineni;
	rm storage_${node}.out;
	rm -rf data;
	mkdir data;
	java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 $port 9999 > storage_${node}.out 2>&1 &
	"
done

ssh bass24 "
cd ~/Documents/courses/cs686/p1-bkommineni;
rm client.out;
rm -rf data;
mkdir data;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client bass01 $port list > client.out;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client bass01 $port store ~/Documents/courses/cs686/p1-bkommineni/clientDirectory/File1.txt > client.out 2>&1 &"
