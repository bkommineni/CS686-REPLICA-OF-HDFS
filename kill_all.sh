node_list=(bass01 bass02 bass03 bass04 bass05 bass06 bass07 bass08 bass09 bass10 bass11 bass12 bass13 bass14 bass15 bass16 bass17 bass18 bass19 bass20 bass21 bass22 bass23 bass24)

for node in ${node_list[@]}; do
	ssh $node "ps -ef | grep 1034 | grep edu.usfca.cs.dfs | awk '{print \$2}' | xargs -I '{}' kill -9 {}"
done
