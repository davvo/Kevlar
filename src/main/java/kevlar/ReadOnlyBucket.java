package kevlar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;

/**
 * A read-only bucket.
 *
 * @author david
 */
class ReadOnlyBucket {

    private final static int MAX_MMAP_SIZE = Integer.MAX_VALUE;

    protected Index index;

    protected File dataFile;

    private MappedByteBuffer[] buffers;

    public ReadOnlyBucket(File dataFile) {
        try {

            this.dataFile = dataFile;

            if (!dataFile.exists()) {
                throw new FileNotFoundException(dataFile.getAbsolutePath());
            }

            // Read index entries
            index = new Index(dataFile);

            // Memory map dataFile
            mmap();

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    protected ReadOnlyBucket() {

    }

    public int size() {
        return index.size();
    }

    public boolean contains(String key) {
        return index.containsKey(key);
    }

    public Collection<String> keys() {
        return index.keySet();
    }

    public long getTimestamp(String key) {
        Index.Entry entry = index.get(key);
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

            Index.Entry entry = index.get(key);

            ByteBufferReader bufReader = new ByteBufferReader(buffers, entry.getOffset());

            Header header = new Header(bufReader);

            bufReader.skip(header.getKeyLength());
            return bufReader.getBytes(header.getValueLength());

        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

    protected void mmap() {
        FileInputStream fis = null;
        FileChannel fc = null;
        try {

            fis = new FileInputStream(dataFile);
            fc = fis.getChannel();

            int numBuffers = (int) (fc.size() / MAX_MMAP_SIZE) + 1;

            buffers = new MappedByteBuffer[numBuffers];
            for (int i = 0; i < numBuffers; ++i) {
                long offset = i * MAX_MMAP_SIZE;
                long size = Math.min(fc.size() - offset, MAX_MMAP_SIZE);
                buffers[i] = fc.map(MapMode.READ_ONLY, offset, size);
            }

        } catch (Exception x) {
            throw new RuntimeException(x);
        } finally {
            Util.close(fc);
            Util.close(fis);
        }
    }

}
