package kevlar;

import java.nio.ByteBuffer;

class ByteBufferReader {

    private ByteBuffer[] buffers;
    private int bufIndex;
    private ByteBuffer buf;

    public ByteBufferReader(ByteBuffer[] buffers, long offset) {
        this.buffers = buffers;
        position(offset);
    }

    public void position(long newPosition) {
        bufIndex = 0;
        buf = buffers[bufIndex];
        while (newPosition > buf.limit()) {
            newPosition -= buf.limit();
            buf = buffers[++bufIndex];
        }
    }

    public void skip(int size) {
        int newPosition = buf.position() + size;
        int limit = buf.limit();
        if (newPosition > limit) {
            buf = buffers[++bufIndex];
            buf.position(newPosition - limit);
        } else {
            buf.position(newPosition);
        }
    }

    public byte[] getBytes(int size) {
        byte[] bytes = new byte[size];
        get(bytes);
        return bytes;
    }

    private void get(byte[] arr) {

        int length = arr.length;
        int available = buf.remaining();

        buf.get(arr, 0, Math.min(available, length));

        if (available < length) {
            buf = buffers[++bufIndex];
            buf.position(0);
            buf.get(arr, available, length - available);
        }

    }
}
