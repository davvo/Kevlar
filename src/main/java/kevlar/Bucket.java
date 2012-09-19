package kevlar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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

    private final static int MAX_BUFFER_SIZE = Integer.MAX_VALUE;

    private Map<String, IndexEntry> index;

    private File dataFile;
    private FileOutputStream dataStream;
    private FileChannel dataChannel;

    private MappedByteBuffer[] buffers;
    private boolean dirty = true;

    public Bucket(File dataFile) {
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

            int bufIndex = (int) (entry.getOffset() / MAX_BUFFER_SIZE);
            int bufOffset = (int) (entry.getOffset() % MAX_BUFFER_SIZE);

            ByteBuffer buf = buffers[bufIndex].duplicate();
            buf.position(bufOffset);

            Header header = new Header(buf);

            int length = header.getValueLength();
            byte[] value = new byte[length];

            int available = buf.remaining();

            buf.get(value, 0, Math.min(available, length));

            if (available < length) {
                buf = buffers[bufIndex + 1].duplicate();
                buf.position(0);
                buf.get(value, available, (length - available));
            }

            return value;

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

            if (index.containsKey(key)) {
                ByteBuffer nil = ByteBuffer.wrap(new byte[] { 0 });
                dataChannel.write(nil, index.get(key).getOffset());
            }

            long offset = dataChannel.position();
            byte[] keyBytes = key.getBytes("utf8");
            Header header = new Header(keyBytes, value);

            dataChannel.write(header.toByteBuffer());
            dataChannel.write(ByteBuffer.wrap(keyBytes));
            dataChannel.write(ByteBuffer.wrap(value));

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

            while (source.position() < source.size()) {
                long position = source.position();
                Header header = new Header(source);
                long count = Header.SIZE + header.getKeyLength() + header.getValueLength();
                if (header.isActive()) {
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

            readIndex();
            flush();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }

    }

    private void readIndex() throws IOException {

        index = new HashMap<String, IndexEntry>();

        FileInputStream fis = new FileInputStream(dataFile);

        FileChannel fc = fis.getChannel();

        ByteBuffer buf = fc.map(MapMode.READ_ONLY, 0, fc.size());

        while (buf.hasRemaining()) {
            long offset = buf.position();
            Header header = new Header(buf);

            if (header.isActive()) {
                byte[] keyBytes = new byte[header.getKeyLength()];
                buf.get(keyBytes);
                String key = new String(keyBytes, "utf8");
                IndexEntry entry = new IndexEntry(header.getTimestamp(), offset);
                index.put(key, entry);
                buf.position(buf.position() + header.getValueLength());
            } else {
                buf.position(buf.position() + header.getKeyLength() + header.getValueLength());
            }

        }
    }

    private synchronized void flush() {
        FileInputStream fis = null;
        FileChannel fc = null;
        try {

            dataChannel.force(true);

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
