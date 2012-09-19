package keyvalue;

import java.util.List;

interface Bucket {

    public int size();

    public boolean contains(String key);

    public List<String> keys();

    public byte[] get(String key);

    public void put(String key, byte[] value);

}
