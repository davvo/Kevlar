package keyvalue;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class KeyValueImpl implements KeyValue {

    private ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<String, Bucket>();

    private File dir;

    public KeyValueImpl(String dirPath) {

        dir = new File(dirPath);

        if (!dir.exists()) {
            dir.mkdirs();
        }

        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dirPath);
        }

        readBuckets();

    }

    private void readBuckets() {
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                return name.endsWith(".idx");
            }
        };

        for (File indexFile : dir.listFiles(filter)) {
            String bucketName = indexFile.getName().substring(0, indexFile.getName().length() - 4);
            File dataFile = new File(dir, bucketName + ".dat");
            buckets.put(bucketName, new BucketImpl(indexFile, dataFile));
        }
    }

    @Override
    public int size() {
        int size = 0;
        for (Bucket bucket : buckets.values()) {
            size += bucket.size();
        }
        return size;
    }

    @Override
    public boolean contains(String bucket, String key) {
        if (buckets.containsKey(bucket)) {
            return buckets.get(bucket).contains(key);
        }
        return false;
    }

    @Override
    public byte[] get(String bucket, String key) {
        if (buckets.containsKey(bucket)) {
            return buckets.get(bucket).get(key);
        }
        return null;
    }

    @Override
    public void put(String bucket, String key, byte[] value) {
        if (!buckets.containsKey(bucket)) {
            File dataFile = new File(dir, bucket + ".dat");
            File indexFile = new File(dir, bucket + ".idx");
            buckets.putIfAbsent(bucket, new BucketImpl(indexFile, dataFile));
        }
        buckets.get(bucket).put(key, value);
    }

    public List<String> keys() {
        List<String> keys = new ArrayList<String>();
        for (Map.Entry<String, Bucket> e : buckets.entrySet()) {
            for (String key : e.getValue().keys()) {
                keys.add(e.getKey() + "/" + key);
            }
        }
        Collections.sort(keys);
        return keys;
    }
}
