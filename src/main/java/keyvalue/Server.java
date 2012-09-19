package keyvalue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Server {

    private final static Pattern PATTERN = Pattern.compile("(GET|PUT|POST) /(\\S+?)/(\\S+)");

    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private KeyValue store;

    private ServerSocketChannel channel;
    private Selector selector;

    public static void main(String[] args) throws IOException {

        new Server("asdf", 8080).start();

    }

    public Server(String dirPath, int port) throws IOException {
        store = new KeyValueImpl(dirPath);

        selector = Selector.open();
        channel = ServerSocketChannel.open();
        channel.socket().bind(new InetSocketAddress("localhost", port));
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("Listening on " + new InetSocketAddress("localhost", port));
    }

    public void start() throws IOException {

        while (true) {

            selector.select();

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {

                SelectionKey key = keys.next();
                keys.remove();

                if (!key.isValid()) {
                    continue;
                }

                if (key.isAcceptable()) {
                    doAccept(key);
                }

                if (key.isReadable()) {
                    doRead(key);
                }

                if (key.isWritable()) {
                    doWrite(key);
                }

            }

        }

    }

    private void doAccept(SelectionKey key) throws IOException {
        SocketChannel client = channel.accept();
        client.configureBlocking(false);
        client.socket().setTcpNoDelay(true);
        client.register(selector, SelectionKey.OP_READ);
    }

    private void doRead(SelectionKey key) throws IOException {
        ReadableByteChannel ch = (ReadableByteChannel) key.channel();
        ByteBuffer buf = ByteBuffer.allocate(1024);

        int read = 0;
        while ((read = ch.read(buf)) > 0) {
            // System.out.println("Read " + read + " bytes");
        }

        String value = new String(buf.array());

        Matcher m = PATTERN.matcher(value);
        if (m.find()) {
            String method = m.group(1);
            if ("GET".equalsIgnoreCase(method)) {
                executor.execute(new Get(key, m.group(2), m.group(3)));
            } else {
                key.attach(new HttpResult("501 Not Implemented", "text/plain", method));
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } else {
            key.attach(new HttpResult("400 Bad Request", "text/plain", "Expected: /<bucket>/<key>"));
            key.interestOps(SelectionKey.OP_WRITE);
        }

    }

    private void doWrite(SelectionKey key) throws IOException {
        SocketChannel sc = (SocketChannel) key.channel();
        HttpResult res = (HttpResult) key.attachment();
        sc.write(res.toByteBuffer());
        sc.close();
    }

    private static class HttpResult {

        private String contentType;
        private String responseCode;
        private byte[] body;

        public HttpResult(String responseCode, String contentType, String body) {
            this(responseCode, contentType, (body + "\n").getBytes());
        }

        public HttpResult(String responseCode, String contentType, byte[] body) {
            this.contentType = contentType;
            this.responseCode = responseCode;
            this.body = body;
        }

        private String getHeaders() {
            StringBuilder sb = new StringBuilder();
            sb.append("HTTP/1.1 ").append(responseCode).append("\n");
            if (contentType != null) {
                sb.append("Content-Type: ").append(contentType).append("\n");
            }
            sb.append("Content-Length: ").append(body.length).append("\n");
            sb.append("\n");
            return sb.toString();
        }

        public ByteBuffer toByteBuffer() {
            byte[] headers = getHeaders().getBytes();
            ByteBuffer buf = ByteBuffer.allocate(headers.length + body.length);
            return (ByteBuffer) buf.put(headers).put(body).rewind();
        }
    }

    private class Get implements Runnable {

        private SelectionKey selectionKey;
        private String bucket;
        private String key;

        public Get(SelectionKey selectionKey, String bucket, String key) {
            this.selectionKey = selectionKey;
            this.bucket = bucket;
            this.key = key;
        }

        @Override
        public void run() {

            byte[] value = store.get(bucket, key);

            if (value == null) {
                selectionKey.attach(new HttpResult("404 Not Found", "text/plain", "Not found: /" + bucket + "/" + key));
            } else {
                selectionKey.attach(new HttpResult("200 OK", null, value));
            }

            selectionKey.interestOps(SelectionKey.OP_WRITE);
        }
    }

}
