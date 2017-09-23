#!/usr/bin/env bash

#Taking latest changes from git and updating project folder on stargate
cd /home4/bkommineni/Documents/courses/cs686/p1-bkommineni

git pull origin master

#Controller (bass01)
cd Documents/courses/cs686/p1-bkommineni;
/usr/local/maven/bin/mvn compile package;
mv /home4/bkommineni/Documents/courses/cs686/p1-bkommineni/target/dfs-1.0-jar-with-dependencies.jar /home4/bkommineni/Documents/courses/cs686/p1-bkommineni/dfs-1.0-jar-with-dependencies.jar;
cd ~/Documents/courses/cs686/p1-bkommineni;
java -cp dfs-1.0-jar-with-dependencies.jar edu.usfca.cs.dfs.Controller 9999 &