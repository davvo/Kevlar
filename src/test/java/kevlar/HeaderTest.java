package kevlar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

public class HeaderTest {

    static File dataFile;

    @BeforeClass
    public static void writeTestData() throws IOException {
        dataFile = File.createTempFile("headerTest", null);
        dataFile.deleteOnExit();

        FileOutputStream fos = new FileOutputStream(dataFile);
        FileChannel fc = fos.getChannel();

        for (int i = 0; i < 100; ++i) {
            byte[] key = ("key" + i).getBytes("utf8");
            byte[] value = ("value" + i).getBytes("utf8");
            if (i % 2 == 0) {
                value = null;
            }
            Header header = new Header(key, value);
            fc.write(header.toByteBuffer());
            fc.write(ByteBuffer.wrap(key));
            if (value != null) {
                fc.write(ByteBuffer.wrap(value));
            }
        }

        fc.force(true);
        fc.close();
        fos.close();
    }

    @Test
    public void testGetKeyLength() {
        Header header = createHeader("key", "value");
        Assert.assertEquals(3, header.getKeyLength());

        header = createHeader("", "value");
        Assert.assertEquals(0, header.getKeyLength());
    }

    @Test
    public void testGetValueLength() {
        Header header = createHeader("key", "value");
        Assert.assertEquals(5, header.getValueLength());

        header = createHeader("key", "");
        Assert.assertEquals(0, header.getValueLength());

        header = createHeader("key", null);
        Assert.assertEquals(0, header.getValueLength());
    }

    @Test
    public void testGetTimestamp() {
        long before = System.nanoTime();
        Header header = createHeader("key", "value");
        long after = System.nanoTime();
        Assert.assertTrue(header.getTimestamp() >= before);
        Assert.assertTrue(header.getTimestamp() <= after);
    }

    @Test
    public void testIsDeleted() {
        Header header = createHeader("key", "value");
        Assert.assertFalse(header.isDeleted());

        header = createHeader("key", "");
        Assert.assertFalse(header.isDeleted());

        header = createHeader("key", null);
        Assert.assertTrue(header.isDeleted());
    }

    @Test
    public void testToByteBuffer() throws UnsupportedEncodingException {

        Header header = createHeader("key", "value");

        ByteBuffer buf = header.toByteBuffer();
        ByteBuffer[] bufArr = { buf };
        ByteBufferReader bbr = new ByteBufferReader(bufArr, 0);

        header = new Header(bbr);

        Assert.assertEquals(3, header.getKeyLength());
        Assert.assertEquals(5, header.getValueLength());
        Assert.assertFalse(header.isDeleted());

    }

    @Test
    public void testFileChannel() throws IOException {

        FileInputStream fis = new FileInputStream(dataFile);
        FileChannel fc = fis.getChannel();

        for (int i = 0; i < 100; ++i) {
            Header header = new Header(fc);
            Assert.assertEquals(3 + String.valueOf(i).length(), header.getKeyLength());
            if (i % 2 == 0) {
                Assert.assertEquals(0, header.getValueLength());
                Assert.assertTrue(header.isDeleted());
            } else {
                Assert.assertEquals(5 + String.valueOf(i).length(), header.getValueLength());
                Assert.assertFalse(header.isDeleted());
            }
            // Skip key and value
            fc.read(ByteBuffer.allocate(header.getKeyLength() + header.getValueLength()));
        }

        fc.close();
        fis.close();

    }

    @Test
    public void testReadKey() throws IOException {

        FileInputStream fis = new FileInputStream(dataFile);
        FileChannel fc = fis.getChannel();

        for (int i = 0; i < 100; ++i) {
            Header header = new Header(fc);
            String key = header.readKey(fc);
            Assert.assertEquals("key" + i, key);
            // Skip value
            fc.read(ByteBuffer.allocate(header.getValueLength()));
        }

        fc.close();
        fis.close();

    }

    private Header createHeader(String key, String value) {
        try {
            byte[] keyBytes = key.getBytes("utf8");
            byte[] valueBytes = (value == null) ? null : value.getBytes("utf8");
            return new Header(keyBytes, valueBytes);
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }

}
