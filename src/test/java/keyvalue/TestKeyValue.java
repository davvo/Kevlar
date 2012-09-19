package keyvalue;

import java.io.IOException;
import java.util.Random;

public class TestKeyValue {

    public static void main(String[] args) throws IOException {

        KeyValueImpl keyValue = new KeyValueImpl("asdf");

        // writeData(keyValue);

        System.out.println(keyValue.keys());

        Random rand = new Random(123);
        for (int i = 0; i < 1000; ++i) {
            int index = rand.nextInt(1000);
            String key = "key" + index;
            String bucket = "bucket" + (index % 10);
            String value = new String(keyValue.get(bucket, key));
            System.out.println(bucket + "/" + key + " => " + value);
        }

    }

    private static void writeData(KeyValue keyValue) {
        for (int i = 0; i < 1000; ++i) {
            String bucket = "bucket" + (i % 10);
            String key = "key" + i;
            byte[] value = ("value" + i).getBytes();
            value = new byte[200];
            keyValue.put(bucket, key, value);
        }
    }
}
