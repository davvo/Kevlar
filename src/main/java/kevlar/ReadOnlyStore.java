package kevlar;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A read-only store.
 *
 * @author david
 */
public class ReadOnlyStore implements Store {

    /**
     * All the buckets.
     */
    protected Map<String, ReadOnlyBucket> buckets;

    /**
     * The data directory.
     */
    protected File dir;

    /**
     * Creates a new read-only store.
     *
     * @param dir path to the directory where files should be stored.
     * @throws FileNotFoundException if directory doesn't exist.
     */
    public ReadOnlyStore(File dir) throws FileNotFoundException {
        this.dir = dir;

        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }

        buckets = new HashMap<String, ReadOnlyBucket>();
        readBuckets(true);
    }

    protected ReadOnlyStore() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        int size = 0;
        for (ReadOnlyBucket bucket : buckets.values()) {
            size += bucket.size();
        }
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(String bucket, String key) {
        if (buckets.containsKey(bucket)) {
            return buckets.get(bucket).contains(key);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] get(String bucket, String key) {
        if (buckets.containsKey(bucket)) {
            return buckets.get(bucket).get(key);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTimestamp(String bucket, String key) {
        if (buckets.containsKey(bucket)) {
            return buckets.get(bucket).getTimestamp(key);
        }
        return -1L;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String bucket, String key, byte[] value) {
        throw new UnsupportedOperationException("Store is read-only");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> keys() {
        List<String> keys = new ArrayList<String>();
        for (Map.Entry<String, ReadOnlyBucket> e : buckets.entrySet()) {
            for (String key : e.getValue().keys()) {
                keys.add(e.getKey() + "/" + key);
            }
        }
        Collections.sort(keys);
        return keys;
    }

    protected void readBuckets(boolean readOnly) {
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                return name.endsWith(".dat");
            }
        };

        for (File dataFile : dir.listFiles(filter)) {
            String bucketName = dataFile.getName().substring(0, dataFile.getName().length() - 4);
            if (readOnly) {
                buckets.put(bucketName, new ReadOnlyBucket(dataFile));
            } else {
                buckets.put(bucketName, new WritableBucket(dataFile));
            }
        }
    }

}
