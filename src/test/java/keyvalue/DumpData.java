package keyvalue;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;

public class DumpData {

    private final static double TILE_SIZE = 256d;
    private final static double INITIAL_RES = 2 * Math.PI * 6378137 / TILE_SIZE;
    private final static double ORIGIN_SHIFT = 2 * Math.PI * 6378137 / 2.0;

    public static void main(String[] args) throws IOException {

        KeyValue store = new KeyValueImpl("asdf");

        System.out.println(store.size());

        String baseUrl = "http://map02.eniro.com/geowebcache/service/tms1.0.0";

        Point2D min = new Point2D.Double(1999180.9190880999, 8240557.177939201);
        Point2D max = new Point2D.Double(2027003.9973799998, 8260430.805290601);

        String[] mapTypes = { "map" };
        int minZoom = 0;
        int maxZoom = 18;

        for (String mapType : mapTypes) {
            for (int zoom = minZoom; zoom <= maxZoom; ++zoom) {
                Point minTile = metersToTile(min, zoom);
                Point maxTile = metersToTile(max, zoom);
                int total = (maxTile.x - minTile.x + 1) * (maxTile.y - minTile.y + 1);
                int done = 0;
                for (int x = minTile.x; x <= maxTile.x; ++x) {
                    for (int y = minTile.y; y <= maxTile.y; ++y) {
                        StringBuilder key = new StringBuilder().append(zoom).append("/").append(x).append("/")
                                .append(y).append(".png");
                        StringBuilder path = new StringBuilder(baseUrl).append("/").append(mapType).append("/")
                                .append(key);
                        byte[] data = load(new URL(path.toString()));
                        // System.out.println(mapType + "/" + key + " -> " +
                        // path);
                        // store.put(mapType, key.toString(), data);
                        if (++done % 100 == 0) {
                            System.out.println(done + "/" + total + ": " + mapType + "/" + key + " -> " + path);
                        }
                    }
                }
            }
        }

    }

    private static byte[] load(URL url) throws IOException {
        URLConnection con = url.openConnection();

        int length = con.getContentLength();

        ByteBuffer buf = ByteBuffer.allocate(length);
        BufferedInputStream in = new BufferedInputStream(con.getInputStream());

        byte[] data = new byte[2048];
        int read = 0;

        while ((read = in.read(data)) > 0) {
            buf.put(data, 0, read);
        }

        in.close();
        return buf.array();
    }

    private static double resolution(int zoom) {
        return INITIAL_RES / Math.pow(2, zoom);
    }

    private static Point metersToTile(Point2D m, int zoom) {
        return pixelsToTile(metersToPixels(m, zoom), zoom);
    }

    private static Point2D metersToPixels(Point2D m, int zoom) {
        double res = resolution(zoom);
        double x = (m.getX() + ORIGIN_SHIFT) / res;
        double y = (m.getY() + ORIGIN_SHIFT) / res;
        return new Point2D.Double(x, y);
    }

    private static Point pixelsToTile(Point2D px, int zoom) {
        int x = (int) Math.ceil(px.getX() / TILE_SIZE) - 1;
        int y = (int) Math.ceil(px.getY() / TILE_SIZE) - 1;
        return new Point(x, y);
    }

}
