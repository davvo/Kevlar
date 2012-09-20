package kevlar;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Bucket {

    public final static int MAX_BUFFER_SIZE = Integer.MAX_VALUE;

    private Map<String, IndexEntry> index;

    private File dataFile;
    private FileOutputStream dataStream;
    private FileChannel dataChannel;
    private long position = 0;

    private MappedByteBuffer[] buffers;
    private boolean dirty = true;

    public Bucket(final File dataFile) {
        try {

            dataFile.createNewFile();

            this.dataFile = dataFile;

            dataStream = new FileOutputStream(dataFile, true);
            dataChannel = dataStream.getChannel();

            readIndex();
            flush();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    public int size() {
        return index.size();
    }

    public boolean contains(String key) {
        return index.containsKey(key);
    }

    public List<String> keys() {
        return Collections.unmodifiableList(new ArrayList<String>(index.keySet()));
    }

    public long getTimestamp(String key) {
        IndexEntry entry = index.get(key);
        if (entry == null) {
            return -1;
        }
        return entry.getTimestamp();
    }

    public byte[] get(String key) {
        try {

            if (!index.containsKey(key)) {
                return null;
            }

            if (dirty) {
                flush();
            }

            IndexEntry entry = index.get(key);

            ByteBufferReader bufReader = new ByteBufferReader(buffers, entry.getOffset());

            Header header = new Header(bufReader);

            bufReader.skip(header.getKeyLength());
            return bufReader.getBytes(header.getValueLength());

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    public synchronized void put(String key, byte[] value) {

        if (key.length() > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Max length of key is " + Byte.MAX_VALUE + ". Was " + key.length());
        }

        if (value.length > MAX_BUFFER_SIZE) {
            throw new IllegalArgumentException("Max length of value is " + MAX_BUFFER_SIZE + " bytes. Was "
                    + value.length);
        }

        try {

            long offset = dataChannel.position();
            byte[] keyBytes = key.getBytes("utf8");
            Header header = new Header(keyBytes, value);

            ByteBuffer buf = ByteBuffer.allocate(Header.SIZE + header.getKeyLength() + header.getValueLength());
            buf.put(header.toByteBuffer());
            buf.put(keyBytes);
            buf.put(value);
            buf.rewind();

            while (buf.hasRemaining()) {
                dataChannel.write(buf);
            }

            IndexEntry entry = new IndexEntry(header.getTimestamp(), offset);
            index.put(key, entry);

            dirty = true;

        } catch (Exception x) {
            throw new RuntimeException(x);
        }

    }

    public synchronized void compact(File newDataFile) {

        try {

            FileInputStream fis = new FileInputStream(dataFile);
            FileChannel source = fis.getChannel();

            newDataFile.createNewFile();
            FileOutputStream fos = new FileOutputStream(newDataFile);
            FileChannel target = fos.getChannel();

            Map<String, IndexEntry> newIndex = new HashMap<String, IndexEntry>(index.size());

            while (source.position() < source.size()) {
                long position = source.position();
                Header header = new Header(source);
                long count = Header.SIZE + header.getKeyLength() + header.getValueLength();
                ByteBuffer keyBuf = ByteBuffer.allocate(header.getKeyLength());
                source.read(keyBuf);
                String key = new String(keyBuf.array(), "utf8");
                boolean active = index.get(key).getOffset() == position;
                if (active) {
                    newIndex.put(key, new IndexEntry(header.getTimestamp(), target.position()));
                    long transferred = 0;
                    while (transferred < count) {
                        transferred += source.transferTo(position + transferred, count - transferred, target);
                    }
                }
                source.position(position + count);
            }

            source.close();
            target.close();

            dataStream.close();
            dataChannel.close();

            dataFile.delete();
            newDataFile.renameTo(dataFile);

            dataStream = new FileOutputStream(dataFile, true);
            dataChannel = dataStream.getChannel();

            index = newIndex;
            writeIndex();

            flush();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }

    }

    private void readIndex() {
        try {

            index = new HashMap<String, IndexEntry>();

            File indexFile = new File(dataFile.getAbsolutePath().substring(0, dataFile.getAbsolutePath().length() - 4)
                    + ".index");

            if (indexFile.exists()) {
                FileInputStream fis = new FileInputStream(indexFile);
                DataInputStream in = new DataInputStream(fis);
                int numEntries = in.readInt();
                for (int i = 0; i < numEntries; ++i) {
                    String key = in.readUTF();
                    long timestamp = in.readLong();
                    long offset = in.readLong();
                    index.put(key, new IndexEntry(timestamp, offset));
                    position = Math.max(position, offset);
                }
                in.close();
                fis.close();
            }

            FileInputStream fis = new FileInputStream(dataFile);

            FileChannel fc = fis.getChannel();

            ByteBuffer buf = fc.map(MapMode.READ_ONLY, position, fc.size() - position);

            while (buf.hasRemaining()) {
                long offset = buf.position();
                Header header = new Header(buf);

                byte[] keyBytes = new byte[header.getKeyLength()];
                buf.get(keyBytes);
                String key = new String(keyBytes, "utf8");

                IndexEntry entry = new IndexEntry(header.getTimestamp(), offset);
                index.put(key, entry);
                buf.position(buf.position() + header.getValueLength());
            }

            position = fc.size();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }

    }

    private void writeIndex() {
        try {

            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            out.writeInt(index.size());
            for (Map.Entry<String, IndexEntry> e : index.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeLong(e.getValue().getTimestamp());
                out.writeLong(e.getValue().getOffset());
            }

            ByteBuffer buf = ByteBuffer.wrap(bout.toByteArray());
            out.close();
            bout.close();

            File indexFile = new File(dataFile.getAbsolutePath().substring(0, dataFile.getAbsolutePath().length() - 4)
                    + ".index");
            FileOutputStream fos = new FileOutputStream(indexFile);
            FileChannel fc = fos.getChannel();

            while (buf.hasRemaining()) {
                fc.write(buf);
            }

            fc.close();
            fos.close();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    private synchronized void flush() {
        FileInputStream fis = null;
        FileChannel fc = null;
        try {

            dataChannel.force(false);

            fis = new FileInputStream(dataFile);
            fc = fis.getChannel();

            int numBuffers = (int) (fc.size() / MAX_BUFFER_SIZE) + 1;

            buffers = new MappedByteBuffer[numBuffers];
            for (int i = 0; i < numBuffers; ++i) {
                long offset = i * MAX_BUFFER_SIZE;
                long size = Math.min(fc.size() - offset, MAX_BUFFER_SIZE);
                buffers[i] = fc.map(MapMode.READ_ONLY, offset, size);
            }

            dirty = false;

        } catch (Exception x) {
            throw new RuntimeException(x);
        } finally {
            Util.close(fc);
            Util.close(fis);
        }
    }

}
