package kevlar;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A store that supports adding key-value mappings.
 *
 * @author david
 */
public class WritableStore extends ReadOnlyStore {

    /**
     * Creates a new writable store.
     *
     * @param dir path to the directory where files should be stored.
     */
    public WritableStore(File dir) {
        this.dir = dir;

        if (!dir.exists()) {
            dir.mkdirs();
        }

        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }

        buckets = new ConcurrentHashMap<String, ReadOnlyBucket>();
        readBuckets(false);
    }

    /**
     * {@inheritDoc}
     */
    public void put(String bucket, String key, byte[] value) {
        if (!buckets.containsKey(bucket)) {
            File dataFile = new File(dir, bucket + ".dat");
            ((ConcurrentMap<String, ReadOnlyBucket>) buckets).putIfAbsent(bucket, new WritableBucket(dataFile));
        }
        ((WritableBucket) buckets.get(bucket)).put(key, value);
    }

    /**
     * Flush written data
     */
    public void flush() {
        for (Map.Entry<String, ReadOnlyBucket> e : buckets.entrySet()) {
            ((WritableBucket) e.getValue()).flush();
        }
    }

    /**
     * Reclaims free space from the data files
     */
    public void compact() {
        for (Map.Entry<String, ReadOnlyBucket> e : buckets.entrySet()) {
            ((WritableBucket) e.getValue()).compact();
        }
    }

}
