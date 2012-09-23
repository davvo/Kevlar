package kevlar;

import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

public class ByteBufferReaderTest {

    @Test
    public void testSingleBuffer() {

        ByteBuffer buf = ByteBuffer.allocate(Byte.MAX_VALUE);
        byte v = 0;
        while (buf.hasRemaining()) {
            buf.put(v++);
        }

        ByteBuffer[] bufArr = { buf };
        ByteBufferReader bbr = new ByteBufferReader(bufArr, 0);

        byte[] bytes = bbr.getBytes(Byte.MAX_VALUE);
        for (int i = 0; i < bytes.length; ++i) {
            Assert.assertEquals(i, bytes[i]);
        }

    }

    @Test
    public void testMultiBuffers() {
        byte v = 0;
        ByteBuffer[] bufArr = new ByteBuffer[5];
        for (int i = 0; i < bufArr.length; ++i) {
            bufArr[i] = ByteBuffer.allocate(20);
            while (bufArr[i].hasRemaining()) {
                bufArr[i].put(v++);
            }
        }

        ByteBufferReader bbr = new ByteBufferReader(bufArr, 0);
        byte[] bytes = bbr.getBytes(100);
        for (int i = 0; i < bytes.length; ++i) {
            Assert.assertEquals(i, bytes[i]);
        }
    }

    @Test
    public void testPosition() {
        // Write 100 integers. 1 integer = 4 bytes.
        ByteBuffer buf = ByteBuffer.allocate(400);
        int v = 0;
        while (buf.hasRemaining()) {
            buf.putInt(v++);
        }
        buf.rewind();

        // Split buffers NOT on exact integer boundaries
        // E.g 16 buffers with 25 bytes each. This means
        // some values are split on separate buffers!
        ByteBuffer[] bufArr = new ByteBuffer[16];
        for (int i = 0; i < 16; ++i) {
            byte[] bytes = new byte[25];
            buf.get(bytes);
            bufArr[i] = ByteBuffer.wrap(bytes);
        }

        ByteBufferReader bbr = new ByteBufferReader(bufArr, 0);

        // Read integers at random positions
        Random rand = new Random();
        for (int i = 0; i < 10000; ++i) {
            int num = rand.nextInt(100);
            bbr.position(num * 4);
            buf = ByteBuffer.wrap(bbr.getBytes(4));
            int num2 = buf.getInt();
            Assert.assertEquals(num, num2);
        }

    }
}
