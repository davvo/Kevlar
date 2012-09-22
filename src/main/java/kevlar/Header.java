package kevlar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class Header {

    static final int SIZE = 13;

    private long timestamp;
    private byte keyLength;
    private int valueLength;

    public Header(byte[] key, byte[] value) {
        timestamp = System.currentTimeMillis();
        keyLength = (byte) key.length;
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

    public String readKey(FileChannel fc) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(keyLength);
        fc.read(buf);
        buf.rewind();
        return new String(buf.array(), "utf8");
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(SIZE);
        buf.putLong(timestamp);
        buf.put(keyLength);
        buf.putInt(valueLength);
        buf.rewind();
        return buf;
    }

    private void fromByteBuffer(ByteBuffer buf) {
        timestamp = buf.getLong();
        keyLength = buf.get();
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
