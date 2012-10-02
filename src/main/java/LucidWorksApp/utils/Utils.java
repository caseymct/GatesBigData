package LucidWorksApp.utils;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonGenerator;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Utils {

    public static String constructUrlParams(HashMap<String,String> params) {
        if (params == null) return "";

        List<String> paramList = new ArrayList<String>();
        for(Map.Entry<String,String> entry : params.entrySet()) {
            paramList.add(entry.getKey() + "=" + entry.getValue());
        }
        return "?" + StringUtils.join(paramList, "&");
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
