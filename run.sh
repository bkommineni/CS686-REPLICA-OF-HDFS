#!/usr/bin/env bash

#Taking latest changes from git and updating project folder on stargate
cd /home4/bkommineni/Documents/courses/cs686/p1-bkommineni

git pull origin master

#Controller (bass01)
ssh bass01 "
cd Documents/courses/cs686/p1-bkommineni;
/usr/local/maven/bin/mvn compile package;
mv /home4/bkommineni/Documents/courses/cs686/p1-bkommineni/target/dfs-1.0-jar-with-dependencies.jar /home4/bkommineni/Documents/courses/cs686/p1-bkommineni/dfs-1.0-jar-with-dependencies.jar;
cd ~/Documents/courses/cs686/p1-bkommineni;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller 9999 &
"
#Storage Nodes (bass02:bass10)
for i in `seq 2 10`; do
	b = "bass"
	z = 0
	if [$i -lt 10]; then
	b = $b$z$i
	else
	b = $b$i
	fi
	ssh b "
	cd Documents/courses/cs686/p1-bkommineni;
	java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
	"
done
#Client
ssh bass11 "
cd Documents/courses/cs686/p1-bkommineni;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client bass01 9999 store clientDirectory/File1.txt &
"
