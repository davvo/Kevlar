package kevlar;

import java.nio.ByteBuffer;

public class ByteBufferReader {

    private ByteBuffer[] buffers;
    private int bufIndex;
    private int bufOffset;

    private ByteBuffer buf;

    public ByteBufferReader(ByteBuffer[] buffers, long offset) {
        this.buffers = buffers;

        bufIndex = (int) (offset / Bucket.MAX_BUFFER_SIZE);
        bufOffset = (int) (offset % Bucket.MAX_BUFFER_SIZE);

        buf = buffers[bufIndex].duplicate();
        buf.position(bufOffset);
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

    public void get(byte[] arr) {

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
