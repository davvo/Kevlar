package kevlar;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class Index implements Map<String, Index.Entry> {

    private File dataFile;
    private File indexFile;

    private Map<String, Entry> entries;

    public Index(File dataFile) {
        this.dataFile = dataFile;
        indexFile = new File(dataFile.getAbsolutePath().substring(0, dataFile.getAbsolutePath().length() - 4)
                + ".index");

        load();
    }

    public void load() {
        try {

            entries = new HashMap<String, Entry>();

            long position = 0;
            if (indexFile.exists()) {
                FileInputStream fis = new FileInputStream(indexFile);
                DataInputStream dis = new DataInputStream(fis);
                int numEntries = dis.readInt();
                for (int i = 0; i < numEntries; ++i) {
                    String key = dis.readUTF();
                    long timestamp = dis.readLong();
                    long offset = dis.readLong();
                    entries.put(key, new Entry(timestamp, offset));
                    position = Math.max(position, offset);
                }
                dis.close();
                fis.close();
            }

            FileInputStream fis = new FileInputStream(dataFile);
            FileChannel fc = fis.getChannel();
            fc.position(position);

            while (fc.position() < fc.size()) {
                Header header = new Header(fc);
                String key = header.readKey(fc);
                entries.put(key, new Entry(header.getTimestamp(), position));
                fc.position(fc.position() + header.getValueLength());
            }

            fc.close();
            fis.close();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }

    }

    public void save() {
        try {

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);

            dos.writeInt(entries.size());
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                dos.writeUTF(e.getKey());
                dos.writeLong(e.getValue().getTimestamp());
                dos.writeLong(e.getValue().getOffset());
            }

            ByteBuffer buf = ByteBuffer.wrap(bos.toByteArray());
            dos.close();
            bos.close();

            FileOutputStream fos = new FileOutputStream(indexFile);
            FileChannel fc = fos.getChannel();

            while (buf.hasRemaining()) {
                fc.write(buf);
            }

            fc.force(true);
            fc.close();
            fos.close();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return entries.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return entries.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<String, Entry>> entrySet() {
        return entries.entrySet();
    }

    @Override
    public Entry get(Object key) {
        return entries.get(key);
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public Set<String> keySet() {
        return entries.keySet();
    }

    @Override
    public Entry put(String key, Entry value) {
        return entries.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Entry> m) {
        entries.putAll(m);
    }

    @Override
    public Entry remove(Object key) {
        return entries.remove(key);
    }

    @Override
    public Collection<Entry> values() {
        return entries.values();
    }

    public static class Entry {

        private long timestamp;
        private long offset;

        public Entry(long timestamp, long offset) {
            this.timestamp = timestamp;
            this.offset = offset;
        }

        /**
         * @return the timestamp
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * @return the offset
         */
        public long getOffset() {
            return offset;
        }

    }

}
