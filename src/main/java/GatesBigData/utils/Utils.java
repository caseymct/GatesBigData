package GatesBigData.utils;

import GatesBigData.constants.Constants;
import model.ValueComparator;
import model.schema.XmlProperty;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;

import static GatesBigData.constants.Constants.*;

public class Utils {
    private static final String FILE_ERROR_MESSAGE  = "<h1>ERROR</h1>";
    private static final Logger logger              = Logger.getLogger(Utils.class);

    public static boolean runningOnProduction() {
        String localhost = "";
        try {
            localhost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
        }
        return true;
        //return SOLR_SERVER.contains(localhost);
    }

    public static boolean isTag(String t1, String t2) {
        return compareStringsIgnoreCaseAndWhitespace(t1, t2);
    }

    public static List<String> getTokens(String d, String delimiter) {
        List<String> tokens = new ArrayList<String>();

        StringTokenizer tk = new StringTokenizer(d, delimiter, true);
        String token = "", prevToken = "";
        int nTokens = tk.countTokens();
        for(int i = 0; i < nTokens; i++) {
            prevToken = token;
            token = (String) tk.nextElement();
            if (!token.equals(delimiter)) {
                tokens.add(token);
            } else {
                if (prevToken.equals(delimiter)) {
                    tokens.add("");
                }
                if (i == nTokens - 1) {
                    tokens.add("");
                }
            }
        }
        return tokens;
    }

    public static <T> Object returnValueIfFieldExists(Map map, String key, T defaultValue) {
        return map.containsKey(key) ? map.get(key) : defaultValue;
    }

    public static <T> T returnIfNotNull(T o, T defaultValue) {
        return o != null ? o : defaultValue;
    }

    public static String replaceHTMLAmpersands(String s) {
        return s.replaceAll("&amp;", "&");
    }

    public static String returnStringValueIfFieldExists(Map m, String field, String defaultValue) {
        return returnValueIfFieldExists(m, field, defaultValue).toString();
    }

    public static <T> boolean nullOrEmpty(T t) {
        if (t == null) return true;

        Method m = null;
        for(String methodName : Arrays.asList("size", "length")) {
            try {
                m = t.getClass().getDeclaredMethod(methodName);
                break;
            } catch (NoSuchMethodException e) {
                m = null;
            }
        }
        if (m != null) {
            m.setAccessible(true);
            try {
                return (Integer) m.invoke(t) == 0;
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage());
            } catch (InvocationTargetException e) {
                logger.error(e.getMessage());
            }
        }

        return false;
    }

    public static boolean fileHasExtension(String file, String ext) {
        if (!ext.startsWith(".")) {
            ext = "." + ext;
        }
        return file.endsWith(ext);
    }

    public static String getUTF8String(byte[] bytes) {
        try {
            return new String(bytes, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            return new String(bytes);
        }
    }

    public static String getUTF8String(String s) {
        return getUTF8String(s.getBytes());
    }

    public static boolean compareStringsIgnoreCaseAndWhitespace(String s1, String s2) {
        return s1 != null && s2 != null && s1.trim().toUpperCase().equals(s2.trim().toUpperCase());
    }

    public static int getInteger(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return INVALID_INTEGER;
        }
    }

    public static void writeJSONArray(String key, List values, JsonGenerator g) throws IOException {
        g.writeArrayFieldStart(key);
        for(Object o : values) {
            writeValueByType(o, g);
        }
        g.writeEndArray();
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
        } else if (value instanceof ArrayList) {
            ArrayList c = (ArrayList) value;
            if (c.size() == 1) {
                g.writeStringField(key, new String(c.get(0).toString().getBytes(), Charset.forName(Constants.UTF8)));
            } else {
                writeJSONArray(key, c, g);
            }
        } else {
            g.writeStringField(key, new String(value.toString().getBytes(), Charset.forName(Constants.UTF8)));
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
            g.writeString(new String(value.toString().getBytes(), Charset.forName(Constants.UTF8)));
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

    public static Document getXmlDocumentFromString(String contents) {
        if (nullOrEmpty(contents)) {
            throw new NullPointerException("Cannot convert null string to Document.");
        }

        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();

        try {
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(IOUtils.toInputStream(contents));
            doc.getDocumentElement().normalize();
            return doc;
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<XmlProperty> getXmlProperties(NodeList nodes) {
        List<XmlProperty> xmlProperties = new ArrayList<XmlProperty>();
        for(int i = 0; i < nodes.getLength(); i++) {
            XmlProperty xmlProperty = new XmlProperty(nodes.item(i));
            if (xmlProperty.isValid()) {
                xmlProperties.add(xmlProperty);
            }
        }
        return xmlProperties;
    }

    public static Document getXmlDocumentFromFile(String filePath) {
        File f = new File(filePath);
        if (!f.exists() || !f.canRead()) {
            throw new RuntimeException(filePath + " has an error; exists: " + f.exists() + ", canRead : " + f.canRead());
        }
        return getXmlDocumentFromString(readFileIntoString(filePath));
    }

    public static String stripFileExtension(String fileName) {
        int index = fileName.lastIndexOf(".");
        return (index == -1) ? fileName : fileName.substring(0, index);
    }

    public static String changeFileExtension(String filePath, String newExt, boolean fullPath) {
        File f = new File(filePath);
        String fileName = f.getName();
        String newFileName = stripFileExtension(fileName) + "." + newExt;

        return fullPath ? new File(f.getParentFile(), newFileName).getPath() : newFileName;
    }


    public static String changeFileExtensionIfFileIsOfType(String filePath, String oldExt, String newExt, boolean fullPath) {
        if (fileHasExtension(filePath, oldExt)) {
            return changeFileExtension(filePath, newExt, fullPath);
        }
        return fullPath ? filePath : new File(filePath).getName();
    }

    public static boolean removeLocalFile(String filePath) {
        return removeLocalFile(new File(filePath));
    }

    public static boolean removeLocalFile(File f) {
        return f.exists() && f.isFile() && f.delete();
    }

    public static boolean writeLocalFile(String filePath, byte[] content) throws IOException {
        return writeLocalFile(new File(filePath), content);
    }

    public static boolean writeLocalFile(File f, byte[] content) throws IOException {
        if (f.exists() && !f.canWrite()) {
            return false;
        }

        FileOutputStream fos = new FileOutputStream(f);
        fos.write(content);
        fos.flush();
        fos.close();
        return f.exists();
    }


    public static void printFileErrorMessage(StringWriter writer, String message) {
        printFileError(writer);
        writer.append("<p>").append(message).append("</p>");
    }

    public static void printFileError(StringWriter writer) {
        writer.append(FILE_ERROR_MESSAGE);
    }

    public static boolean hasFileErrorMessage(String s) {
        return s.contains(FILE_ERROR_MESSAGE);
    }

    public static String getFileErrorString() {
        return FILE_ERROR_MESSAGE;
    }


    public static void closeResource(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static Map<String, Number> sortByValue(Map<String, Number> map) {
        ValueComparator bvc = new ValueComparator(map);
        TreeMap<String, Number> sortedMap = new TreeMap<String, Number>(bvc);
        sortedMap.putAll(map);
        return sortedMap;
    }
}
