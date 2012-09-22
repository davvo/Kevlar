package kevlar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A writable bucket.
 *
 * @author david
 */
class WritableBucket extends ReadOnlyBucket {

    private static final int DEFAULT_BUFFER_LIMIT = 4096;

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private FileChannel dataChannel;

    private Map<String, byte[]> buffer = new HashMap<String, byte[]>();
    private int bufferSize = 0;
    private int bufferLimit = DEFAULT_BUFFER_LIMIT;

    public WritableBucket(File dataFile, int bufferLimit) {
        this(dataFile);
        this.bufferLimit = bufferLimit;
    }

    public WritableBucket(File dataFile) {
        FileOutputStream fos = null;
        try {

            this.dataFile = dataFile;

            if (!dataFile.exists()) {
                dataFile.createNewFile();
            }

            fos = new FileOutputStream(dataFile, true);
            dataChannel = fos.getChannel();
            dataChannel.lock();

            // Read index entries
            index = new Index(dataFile);

            // Memory map dataFile
            mmap();

        } catch (Exception x) {
            throw new RuntimeException(x);
        } finally {
            // Util.close(fos);
        }
    }

    @Override
    public byte[] get(String key) {
        try {
            lock.readLock().lock();
            return super.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void mmap() {
        try {
            lock.writeLock().lock();
            super.mmap();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public synchronized void put(String key, byte[] value) {
        try {
            buffer.put(key, value);
            bufferSize += Header.SIZE + key.getBytes("utf8").length + value.length;

            if (bufferSize > bufferLimit) {
                flush();
            }

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    public synchronized void compact() {
        FileInputStream fis = null;
        FileOutputStream fos = null;
        FileChannel source = null;
        FileChannel target = null;
        try {

            flush();

            fis = new FileInputStream(dataFile);
            source = fis.getChannel();

            File newDataFile = File.createTempFile(dataFile.getName(), null, dataFile.getParentFile());
            fos = new FileOutputStream(newDataFile);
            target = fos.getChannel();
            target.lock();

            Map<String, Index.Entry> indices = new HashMap<String, Index.Entry>(index.size());

            while (source.position() < source.size()) {
                long position = source.position();
                Header header = new Header(source);
                long count = Header.SIZE + header.getKeyLength() + header.getValueLength();
                String key = header.readKey(source);
                boolean active = index.get(key).getOffset() == position;
                if (active) {
                    indices.put(key, new Index.Entry(header.getTimestamp(), target.position()));
                    long transferred = 0;
                    while (transferred < count) {
                        transferred += source.transferTo(position + transferred, count - transferred, target);
                    }
                }
                source.position(position + count);
            }

            target.force(true);

            lock.writeLock().lock();

            dataFile.delete();
            newDataFile.renameTo(dataFile);

            dataChannel.close();
            dataChannel = target;
            target = null;

            index.clear();
            index.putAll(indices);
            index.save();

            mmap();

        } catch (Exception x) {
            throw new RuntimeException(x);
        } finally {
            Util.close(target, source);
            Util.close(fis);
            Util.close(fos);
            lock.writeLock().unlock();
        }

    }

    public synchronized void flush() {
        try {

            Map<String, Index.Entry> indices = new HashMap<String, Index.Entry>(buffer.size());

            for (Map.Entry<String, byte[]> e : buffer.entrySet()) {

                String key = e.getKey();
                byte[] value = e.getValue();

                if (key.length() > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Max length of key is " + Byte.MAX_VALUE + ". Was "
                            + key.length());
                }

                if (value.length > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Max length of value is " + Integer.MAX_VALUE + " bytes. Was "
                            + value.length);
                }

                byte[] keyBytes = key.getBytes("utf8");
                Header header = new Header(keyBytes, value);

                indices.put(key, new Index.Entry(System.currentTimeMillis(), dataChannel.position()));

                dataChannel.write(header.toByteBuffer());
                dataChannel.write(ByteBuffer.wrap(keyBytes));
                dataChannel.write(ByteBuffer.wrap(value));
            }

            dataChannel.force(true);

            index.putAll(indices);
            index.save();

            mmap();

            buffer.clear();
            bufferSize = 0;

        } catch (Exception x) {
            throw new RuntimeException(x);
        }

    }

}
