package LucidWorksApp.utils;

import org.codehaus.jackson.JsonGenerator;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


public class Utils {
    public static final String COLLECTIONS_ENDPOINT = "/api/collections";
    public static final String SERVER = "http://denlx006.dn.gates.com:8983";
    //public static final String SERVER = "http://localhost:8983";
    public static final String DATASOURCES_ENDPOINT = "/datasources";
    public static final String INFO_ENDPOINT = "/info";
    public static final String INDEX_ENDPOINT = "/index";
    public static final String SOLR_ENDPOINT = "/solr";
    public static final String UPDATECSV_ENDPOINT = "/update/csv";
    public static final String LUKE_ENDPOINT = "/admin/luke";
    public static final String SOLR_PATH = "/Users/caseymctaggart/projects/solr/example/solr";
    public static final String SOLR_SCHEMA_HDFSKEY = "HDFSKey";

    public static String getServer() {
        return SERVER;
    }

    public static String getSolrEndpoint() {
        return SOLR_ENDPOINT;
    }

    public static String getUpdateCsvEndpoint() {
        return UPDATECSV_ENDPOINT;
    }

    public static String getLukeEndpoint() {
        return LUKE_ENDPOINT;
    }

    public static String getSolrPath() {
        return SOLR_PATH;
    }

    public static String getSolrSchemaHdfskey() {
        return SOLR_SCHEMA_HDFSKEY;
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

    public static void writeValueByType(Object value, JsonGenerator g) throws IOException {
        if (value == null) {
            g.writeNull();
        } else if (value instanceof Boolean) {
            g.writeBoolean((Boolean) value);
        } else if (value instanceof Number) {
            if (value instanceof Integer) {
                g.writeNumber((Integer) value);
            } else if (value instanceof Double) {
                g.writeNumber((Double) value);
            } else if (value instanceof Float) {
                g.writeNumber((Float) value);
            } else if (value instanceof Long) {
                g.writeNumber((Long) value);
            }
        } else {
            g.writeString(new String(value.toString().getBytes(), Charset.forName("UTF-8")));
        }
    }

    public static String readFileIntoString(String fileName) {
        StringBuilder sb = new StringBuilder();

        try {
            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;

            while ((line = br.readLine()) != null)   {
                sb.append(line);
            }

            in.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return "File not found";
        }

        return sb.toString();
    }
}
