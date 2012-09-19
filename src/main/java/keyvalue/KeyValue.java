package keyvalue;

public interface KeyValue {

    public int size();

    public boolean contains(String bucket, String key);

    public byte[] get(String bucket, String key);

    public void put(String bucket, String key, byte[] value);

}
