import java.util.TreeMap;
import java.util.SortedMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;



public class ConsisentHashRing {
    //actual ring structure
    private final SortedMap<Long, Integer> ring;
    //number of entries each node should have in the ring
    private final int virtualNodes;
    //hash function being used
    private final HashFunction hashFunction;
    /**
     * 
     * @param virtualNodes number of entries in ring for each node
     * 
     * constructor accepts number of nodes, then passes it to a constructor with a specified hash function
     */
    public ConsisentHashRing(int virtualNodes) {
        this(virtualNodes, Hashing.murmur3_128());
    }

    /**
     * 
     * @param virtualNodes number of entries in ring for each node
     * @param hf the hash function
     * 
     * normal constructor
     */
    public ConsisentHashRing(int virtualNodes, HashFunction hf) {
        this.ring = new TreeMap<>();
        this.virtualNodes = virtualNodes;
        this.hashFunction = hf;
    }
    
    /**
     * 
     * @param n the port num of the node to be added
     * 
     * adds a node, and all virtual nodes to the ring
     */
    public void add(int n) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = computeHash(n + "#" + i);
            ring.put(hash, n);
        }
    }

    /**
     * 
     * @param n the port num of the node to be added
     * 
     * removed all nodes, and their virtual nodes from the ring
     */
    public void remove(int n) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = computeHash(n + "#" + i);
            ring.remove(hash);
        }
    }

    /**
     * 
     * @param key the key that is being requested
     * @return the port number of the node that is responsible for this key
     * 
     * returns -1 if the ring is empty
     * looks clockwise through the ring, returning the next virtual node encountered
     * if it reaches the end, loop back round
     */
    public int getNode(String key) {
        //if empty
        if (ring.isEmpty()) {
            return -1;
        }

        long hash = computeHash(key);
        SortedMap<Long, Integer> tailMap = ring.tailMap(hash);
        if (tailMap.isEmpty()) {
            return ring.get(ring.firstKey());
        } else {
            return tailMap.get(tailMap.firstKey());
        }
    }

    /**
     * 
     * @param key the key to be hashed
     * @return the hash
     * uses the hashString of the hashfunction previously decided
     */
    private long computeHash(String key) {
        return hashFunction.hashString(key, StandardCharsets.UTF_8).asLong();
    }
}
