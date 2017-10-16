#!/bin/bash
node_list=(bass01 bass02 bass04 bass07 bass08)

for node in ${node_list[@]}; do
	ssh $node "ps -ef | grep 1034 | grep edu.usfca.cs.dfs | awk '{print \"kill -9 \"\$2}' | bash"
done
