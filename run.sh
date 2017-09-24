#!/usr/bin/env bash

node_list=(bass02 bass04 bass07 bass08 bass09 bass11)

#Controller (bass01)
ssh bass01 "
cd ~/Documents/courses/cs686/p1-bkommineni;
/usr/local/maven/bin/mvn compile package;
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller 9000 > controller.out 2>&1 &
"

#StorageNodes

for node in ${node_list[@]};do
	ssh $node "
	cd ~/Documents/courses/cs686/p1-bkommineni/target/;
	rm storage_${node}.out;
	java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9000 > storage_${node}.out 2>&1 &
	"
done

#Client
ssh bass12 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client bass01 9000 store ~/Documents/courses/cs686/p1-bkommineni/clientDirectory/File1.txt > client.out 2>&1 &
"
