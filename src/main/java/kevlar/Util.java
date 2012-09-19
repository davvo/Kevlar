package kevlar;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

class Util {

    static void close(FileChannel fc) {
        try {
            if (fc != null) {
                fc.close();
            }
        } catch (IOException x) {
            // Ignore
        }
    }

    static void close(FileInputStream fis) {
        try {
            if (fis != null) {
                fis.close();
            }
        } catch (IOException x) {
            // Ignore
        }
    }

}
