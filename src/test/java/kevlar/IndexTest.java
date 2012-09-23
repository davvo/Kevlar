package kevlar;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IndexTest {

    static File dataFile;
    Index index;

    @BeforeClass
    public static void createTestData() throws IOException {
        dataFile = File.createTempFile("indexTest", null);

        Index index = new Index(dataFile);

        for (int i = 0; i < 10; ++i) {
            index.put("key" + i, new Index.Entry(i, i));
        }

        index.save();
    }

    @AfterClass
    public static void deleteTestData() throws IOException {
        if (dataFile != null) {
            Index index = new Index(dataFile);
            index.delete();

            dataFile.delete();
        }

    }

    @Before
    public void testLoad() {
        index = new Index(dataFile);
        index.load();
    }

    @Test
    public void testSize() {
        Assert.assertEquals(10, index.size());
    }

    @Test
    public void testClear() {
        Assert.assertEquals(10, index.size());
        index.clear();
        Assert.assertEquals(0, index.size());
    }

    @Test
    public void testContainsKey() {
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue(index.containsKey("key" + i));
        }
        Assert.assertFalse(index.containsKey("xxx"));
        Assert.assertFalse(index.containsKey(""));
    }

    @Test
    public void testGet() {
        for (int i = 0; i < 10; ++i) {
            Assert.assertNotNull(index.get("key" + i));
        }
        Assert.assertNull(index.get("xxx"));
        Assert.assertNull(index.get(""));
    }

    @Test
    public void testKeySet() {
        Set<String> keySet = index.keySet();
        Assert.assertEquals(10, keySet.size());
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue(keySet.contains("key" + i));
        }
        Assert.assertFalse(keySet.contains("xxx"));
        Assert.assertFalse(keySet.contains(""));
    }

    @Test
    public void testPut() {
        Assert.assertFalse(index.containsKey("key10"));
        index.put("key10", new Index.Entry(0, 0));
        Assert.assertTrue(index.containsKey("key10"));
    }

    @Test
    public void testPutAll() {
        Assert.assertFalse(index.containsKey("key10"));
        Assert.assertFalse(index.containsKey("key11"));
        Assert.assertFalse(index.containsKey("key12"));
        Map<String, Index.Entry> m = new HashMap<String, Index.Entry>();
        m.put("key10", new Index.Entry(0, 0));
        m.put("key11", new Index.Entry(0, 0));
        m.put("key12", new Index.Entry(0, 0));
        index.putAll(m);
        Assert.assertTrue(index.containsKey("key10"));
        Assert.assertTrue(index.containsKey("key11"));
        Assert.assertTrue(index.containsKey("key12"));
    }

    @Test
    public void testRemove() {
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue(index.containsKey("key" + i));
            index.remove("key" + i);
            Assert.assertFalse(index.containsKey("key" + i));
        }
        Assert.assertEquals(0, index.size());
    }

}
