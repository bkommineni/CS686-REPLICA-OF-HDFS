package edu.usfca.cs.dfs;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bharu on 9/10/17.
 */
public class BlockMetadata {

    private String filename;
    private int blockId;
    private List<ReplicaMetadata> replicaMetadatas = new ArrayList<>();
}
