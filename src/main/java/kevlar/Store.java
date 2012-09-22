package kevlar;

import java.util.Collection;

/**
 * A high-performance key-value store.
 *
 * @author david
 */
public interface Store {

    /**
     * Gets the value of a mapping.
     * @param bucket the bucket
     * @param key the key
     * @return the corresponding value of key, or null if the mapping doesn't exist.
     */
    public byte[] get(String bucket, String key);

    /**
     * Gets the timestamp of a mapping.
     * @param bucket the bucket
     * @param key the key
     * @return the time when the mapping of key was created or updated, or -1 if
     * the mapping doesn't exist.
     */
    public long getTimestamp(String bucket, String key);

    /**
     * Create a new mapping. If the bucket doesn't exist, it will be
     * created.
     * @param bucket the bucket
     * @param key the key
     * @param value the value
     */
    public void put(String bucket, String key, byte[] value);

    /**
     * @return the size of all key-value mappings in the store.
     */
    public int size();

    /**
     * Tests if a mapping exists.
     * @param bucket the bucket
     * @param key the key
     * @return true if the mapping of <code>key</code> exist inside <code>bucket</code>, false otherwise.
     */
    public boolean contains(String bucket, String key);

    /**
     * @return a list of all keys in the store
     */
    public Collection<String> keys();

}
