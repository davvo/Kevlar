package kevlar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class Header {

    public static final int SIZE = 16;

    private long timestamp;
    private int keyLength;
    private int valueLength;

    public Header(byte[] key, byte[] value) {
        timestamp = System.currentTimeMillis();
        keyLength = key.length;
        valueLength = value.length;
    }

    public Header(FileChannel ch) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(SIZE);
        ch.read(buf);
        buf.rewind();
        fromByteBuffer(buf);
    }

    public Header(ByteBufferReader bufRead) {
        ByteBuffer buf = ByteBuffer.wrap(bufRead.getBytes(SIZE));
        fromByteBuffer(buf);
    }

    public Header(ByteBuffer buf) {
        fromByteBuffer(buf);
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(SIZE);
        buf.putLong(timestamp);
        buf.putInt(keyLength);
        buf.putInt(valueLength);
        buf.rewind();
        return buf;
    }

    private void fromByteBuffer(ByteBuffer buf) {
        timestamp = buf.getLong();
        keyLength = buf.getInt();
        valueLength = buf.getInt();
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the keyLength
     */
    public int getKeyLength() {
        return keyLength;
    }

    /**
     * @return the valueLength
     */
    public int getValueLength() {
        return valueLength;
    }

}
