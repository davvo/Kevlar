package keyvalue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class BucketImpl implements Bucket {

    private final static int MAX_BUFFER_SIZE = Integer.MAX_VALUE;

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Map<String, Long> indices;

    private File dataFile;
    private File indexFile;

    private FileChannel dataChannel;
    private FileChannel indexChannel;

    private MappedByteBuffer[] buffers;
    private boolean dirty = true;

    public BucketImpl(File indexFile, File dataFile) {
        try {

            dataFile.createNewFile();
            indexFile.createNewFile();

            this.dataFile = dataFile;
            this.indexFile = indexFile;

            readIndexFile();
            initChannels();

            flush();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    @Override
    public int size() {
        return indices.size();
    }

    @Override
    public boolean contains(String key) {
        return indices.containsKey(key);
    }

    @Override
    public byte[] get(String key) {
        try {

            lock.readLock().lock();

            if (dirty) {
                flush();
            }

            if (!indices.containsKey(key)) {
                return null;
            }

            long offset = indices.get(key);

            int bufIndex = (int) (offset / MAX_BUFFER_SIZE);
            int bufOffset = (int) (offset % MAX_BUFFER_SIZE);

            ByteBuffer buf = buffers[bufIndex].duplicate();
            buf.position(bufOffset);

            int length = buf.getInt();
            byte[] bytes = new byte[length];

            int available = buf.remaining();

            buf.get(bytes, 0, Math.min(available, length));

            if (available < length) {
                buf = buffers[bufIndex + 1].duplicate();
                buf.position(0);
                buf.get(bytes, available, (length - available));
            }

            return bytes;

        } catch (Exception x) {
            throw new RuntimeException(x);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(String key, byte[] value) {

        if (value.length > MAX_BUFFER_SIZE) {
            throw new IllegalArgumentException("Max length of value is " + MAX_BUFFER_SIZE + " bytes. Was "
                    + value.length);
        }

        try {
            lock.writeLock().lock();

            byte[] keyBytes = key.getBytes();
            long offset = dataChannel.position();

            ByteBuffer buf = ByteBuffer.allocate(4 + value.length);
            buf.putInt(value.length);
            buf.put(value);
            buf.rewind();
            dataChannel.write(buf);

            buf = ByteBuffer.allocate(4 + keyBytes.length + 8);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            buf.putLong(offset);
            buf.rewind();
            indexChannel.write(buf);

            indices.put(key, offset);
            dirty = true;

        } catch (Exception x) {
            throw new RuntimeException(x);
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override
    public List<String> keys() {
        return new ArrayList<String>(indices.keySet());
    }

    private void initChannels() throws IOException {
        FileOutputStream out = new FileOutputStream(dataFile, true);
        dataChannel = out.getChannel();
        dataChannel.position(dataChannel.size());

        out = new FileOutputStream(indexFile, true);
        indexChannel = out.getChannel();
        indexChannel.position(indexChannel.size());
    }

    private void readIndexFile() throws IOException {

        FileInputStream fis = new FileInputStream(indexFile);
        FileChannel fc = fis.getChannel();
        ByteBuffer buf = fc.map(MapMode.READ_ONLY, 0, fc.size());

        indices = new HashMap<String, Long>();
        while (buf.hasRemaining()) {
            int length = buf.getInt();
            byte[] bytes = new byte[length];
            buf.get(bytes);
            String key = new String(bytes);
            long offset = buf.getLong();
            indices.put(key, offset);
        }
    }

    private void flush() {
        FileInputStream fis = null;
        FileChannel fc = null;
        try {

            dataChannel.force(true);
            indexChannel.force(true);

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
            close(fc);
            close(fis);
        }
    }

    private void close(FileChannel fc) {
        try {
            if (fc != null) {
                fc.close();
            }
        } catch (IOException x) {
            // Ignore
        }
    }

    private void close(FileInputStream fis) {
        try {
            if (fis != null) {
                fis.close();
            }
        } catch (IOException x) {
            // Ignore
        }
    }

}
