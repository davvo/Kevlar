package kevlar;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

class Util {

    static void close(FileChannel... fcs) {
        for (FileChannel fc : fcs) {
            try {
                if (fc != null) {
                    fc.close();
                }
            } catch (IOException x) {
                // Ignore
            }
        }
    }

    static void close(InputStream... ins) {
        for (InputStream in : ins) {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException x) {
                // Ignore
            }
        }
    }

    static void close(OutputStream... outs) {
        for (OutputStream out : outs) {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException x) {
                // Ignore
            }
        }
    }

    static void release(FileLock lock) {
        try {
            if (lock != null) {
                lock.release();
            }
        } catch (IOException x) {
            // Ignore
        }
    }
}
