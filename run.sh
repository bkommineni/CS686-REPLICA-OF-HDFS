#!/usr/bin/env bash

#Controller (bass01)
ssh bass01 "
cd ~/Documents/courses/cs686/p1-bkommineni;
/usr/local/maven/bin/mvn compile package;
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller 9999 &
"

#StorageNodes
ssh bass02 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass03 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass04 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass05 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass06 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass07 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass08 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass09 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass10 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass11 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass12 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass13 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass14 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass15 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass16 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass17 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass18 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass19 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass20 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass21 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass22 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
ssh bass23 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.StorageNode bass01 9999 &
"
#Client
ssh bass24 "
cd ~/Documents/courses/cs686/p1-bkommineni/target/;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Client bass01 9999 store clientDirectory/File1.txt &
"