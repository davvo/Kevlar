package keyvalue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class KeyValueServlet extends HttpServlet {

    private KeyValue store;

    @Override
    public void init(ServletConfig config) {
        String dirPath = config.getInitParameter("dirPath");
        store = new KeyValueImpl(dirPath);
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {

        Path path;

        try {
            path = getPath(req);
        } catch (Exception x) {
            res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Expected " + req.getServletPath() + "/<bucket>/<key>");
            return;
        }

        if (!store.contains(path.bucket, path.key)) {
            res.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        byte[] value = store.get(path.bucket, path.key);

        res.setContentLength(value.length);

        String contentType = req.getHeader("Content-Type");
        if (contentType != null) {
            res.setContentType(contentType);
        }

        OutputStream out = res.getOutputStream();
        out.write(value);
        out.flush();
        out.close();

    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {

        Path path;

        try {
            path = getPath(req);
        } catch (Exception x) {
            res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Expected " + req.getServletPath() + "/<bucket>/<key>");
            return;
        }

        ServletInputStream in = req.getInputStream();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        byte[] bytes = new byte[2048];
        int read = 0;
        while ((read = in.read(bytes)) > 0) {
            bout.write(bytes, 0, read);
        }
        bout.flush();
        bout.close();

        store.put(path.bucket, path.key, bout.toByteArray());
    }

    @Override
    public void doPut(HttpServletRequest req, HttpServletResponse res) throws IOException {
        doPost(req, res);
    }

    private Path getPath(HttpServletRequest req) {
        String pathInfo = req.getPathInfo().substring(1);
        int index = pathInfo.indexOf("/");
        String bucket = pathInfo.substring(0, index);
        String key = pathInfo.substring(index + 1);
        return new Path(bucket, key);
    }

    private static class Path {
        String bucket;
        String key;

        public Path(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }
}
