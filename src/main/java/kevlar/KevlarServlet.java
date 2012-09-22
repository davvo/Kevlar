package kevlar;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class KevlarServlet extends HttpServlet {

    private WritableStore store;

    @Override
    public void init(ServletConfig config) {
        File dir = new File(config.getInitParameter("dirPath"));
        store = new WritableStore(dir);
    }

    @Override
    public long getLastModified(HttpServletRequest req) {
        return store.getTimestamp(bucket(req), key(req));
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {

            String bucket = bucket(req);
            String key = key(req);

            if (!store.contains(bucket, key)) {
                res.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }

            byte[] value = store.get(bucket, key);

            res.setContentLength(value.length);

            String contentType = req.getHeader("Content-Type");
            if (contentType != null) {
                res.setContentType(contentType);
            }

            OutputStream out = res.getOutputStream();
            out.write(value);
            out.flush();
            out.close();

        } catch (IllegalArgumentException x) {
            res.sendError(HttpServletResponse.SC_BAD_REQUEST, x.getMessage());
        }
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {

            ServletInputStream in = req.getInputStream();
            ByteArrayOutputStream bout = new ByteArrayOutputStream();

            byte[] bytes = new byte[2048];
            int read = 0;
            while ((read = in.read(bytes)) > 0) {
                bout.write(bytes, 0, read);
            }
            bout.flush();
            bout.close();

            store.put(bucket(req), key(req), bout.toByteArray());

        } catch (IllegalArgumentException x) {
            res.sendError(HttpServletResponse.SC_BAD_REQUEST, x.getMessage());
        }
    }

    @Override
    public void doPut(HttpServletRequest req, HttpServletResponse res) throws IOException {
        doPost(req, res);
    }

    private String bucket(HttpServletRequest req) {
        try {
            String pathInfo = req.getPathInfo().substring(1);
            return pathInfo.substring(0, pathInfo.indexOf("/"));
        } catch (Exception x) {
            throw new IllegalArgumentException("Expected " + req.getServletPath() + "/<bucket>/<key>");
        }
    }

    private String key(HttpServletRequest req) {
        try {
            String pathInfo = req.getPathInfo().substring(1);
            return pathInfo.substring(pathInfo.indexOf("/") + 1);
        } catch (Exception x) {
            throw new IllegalArgumentException("Expected " + req.getServletPath() + "/<bucket>/<key>");
        }
    }

}
