#!/usr/bin/env bash

node_list=(bass02 bass03 bass04 bass05 bass06 bass07 bass08 bass09 bass10 bass11 bass12 bass13 bass14 bass15 bass16 bass17 bass18 bass19 bass20)

#Controller (bass01)
ssh bass01 "
cd ~/Documents/courses/cs686/p1-bkommineni;
/usr/local/maven/bin/mvn compile package;
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller 9999 > controller.out 2>&1 &
"

#StorageNodes

for node in ${node_list[@]};do
	ssh $node "
	cd ~/Documents/courses/cs686/p1-bkommineni/target/;
	java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 > storage.out 2>&1 &
	"
done

#Client
ssh bass21 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client bass01 9999 store clientDirectory/File1.txt > client.out 2>&1 &
"
