package LucidWorksApp.utils;

import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: caseymctaggart
 * Date: 7/26/12
 * Time: 2:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utils {
    public static final String COLLECTIONS_ENDPOINT = "/api/collections";
    //public static final String SERVER = "http://denlx006.dn.gates.com:8888";
    public static final String SERVER = "http://localhost:8888";
    public static final String COLLECTIONS_TEMPLATES_ENDPOINT = "/api/collectiontemplates";
    public static final String DATASOURCES_ENDPOINT = "/datasources";
    public static final String INFO_ENDPOINT = "/info";
    public static final String INDEX_ENDPOINT = "/index";

    public static String getServer() {
        return SERVER;
    }

    public static List<String> convertObjectListToStringList(List<Object> objectList) {
        List<String> stringList = new ArrayList<String>();
        for (Object o : objectList) {
            if (o instanceof String) {
                stringList.add((String) o);
            }
        }
        return stringList;
    }

    public static void writeValueByType(String key, Object value, JsonGenerator g) throws IOException {
        if (value == null) {
            g.writeNullField(key);
        } else if (value instanceof Boolean) {
            g.writeBooleanField(key, (Boolean) value);
        } else if (value instanceof Number) {
            if (value instanceof Integer) {
                g.writeNumberField(key, (Integer) value);
            } else if (value instanceof Double) {
                g.writeNumberField(key, (Double) value);
            } else if (value instanceof Float) {
                g.writeNumberField(key, (Float) value);
            } else if (value instanceof Long) {
                g.writeNumberField(key, (Long) value);
            }
        } else {
            g.writeStringField(key, new String(value.toString().getBytes(), Charset.forName("UTF-8")));
        }
    }
}
