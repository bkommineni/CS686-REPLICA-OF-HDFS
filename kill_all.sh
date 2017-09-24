#!/bin/bash
node_list=(bass01 bass02 bass04 bass07 bass08 bass09 bass11 bass12 bass14 bass15 bass16 bass17 bass18 bass20 bass21 bass22 bass23)

for node in ${node_list[@]}; do
	ssh $node "ps -ef | grep 1034 | grep edu.usfca.cs.dfs | awk '{print \"kill -9 \"\$2}' | bash"
done
