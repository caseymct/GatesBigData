package service.solrReindexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.DateField;

import java.io.*;
import java.net.URI;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReindexerUtils {
    public static final String DEFAULT_DELIMITER        = "~";
    public static final String SOLR_URL_BASE            = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String SOLR_KEY_ID              = "id";
    public static final String SOLR_KEY_TITLE           = "title";
    public static final String SOLR_KEY_URL             = "url";
    public static final String LOG_FILE                 = "/tmp/tsvlog";

    public static final String CONF_KEY_CORENAME        = "CORENAME";
    public static final String CONF_KEY_FIELDS          = "FIELDS";
    public static final String CONF_KEY_DELIMITER       = "DELIMITER";
    public static final String CONF_KEY_LOGFILE         = "LOGFILE";
    public static final String CONF_KEY_INPUTFILE_PATH  = "INPUTFILE_PATH";
    public static final String CONF_KEY_WRITE_INPUTFILE = "WRITE_INPUTFILE";
    public static final String CONF_KEY_DELETE_INDEX    = "DELETE_INDEX";
    public static final String CONF_KEY_SOLR_URL        = "SOLR_URL";
    public static final String CONF_KEY_HDFS_INPUTFILE  = "HDFS_INPUTFILE";
    public static final String CONF_KEY_ERROR_STRING    = "ERROR";
    public static final String CONF_KEY_DEBUG           = "DEBUG";

    public static final String JOB_MAP_KEY_FIELDS       = "FIELDS";
    public static final String JOB_MAP_KEY_SOLRURL      = "SOLR_URL";
    public static final String JOB_MAP_KEY_DELIMITER    = "DELIMITER";
    public static final String JOB_MAP_KEY_LOGFILE      = "LOGFILE";

    public static final String ERP_DB_USERNAME          = "Look";
    public static final String ERP_DB_PASSWORD          = "look";
    public static final String ERP_DB_USERNAME_KEY      = "user";
    public static final String ERP_DB_PASSWORD_KEY      = "password";
    public static final String JDBC_DRIVER_CLASS        = "oracle.jdbc.OracleDriver";
    public static final String JDBC_SERVER              = "jdbc:oracle:thin:@//erpdbadb.dn.gates.com:1591/ERPDBA";

    public static final String HDFS_URI                 = "hdfs://denlx006.dn.gates.com:8020";
    public static final String HDFS_USERNAME            = "hdfs";
    public static final String HDFS_USER_PATH           = "/user/hdfs/";
    public static final String HDFS_FIELDS_FILENAME     = "fields.csv";

    public static final String SQL_TYPE_STRING          = "VARCHAR2";

    public static String lineSeparator                  = System.getProperty("line.separator");
    public static byte[] newline                        = lineSeparator.getBytes();

    public static Pattern genericDatePattern = Pattern.compile("^(\\d{2}|\\d{4}|\\w{3})(-|\\s)(\\w{3}|\\d{2})(-|\\s)(\\d{2}|\\d{4})(T|\\s){0,1}(\\d{2}:\\d{2}){0,1}(:\\d{2}){0,1}(\\.\\d{3}){0,1}(Z){0,1}$");

    public static List<String> monthAbbrevs = Arrays.asList("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                                                            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");

    private static HashMap<String, String> updateStatements = new HashMap<String, String>() {{
        put("AR_data",      "select * from apps.xxar_trx_date_1");
        put("AR_data_test", "select * from apps.xxar_trx_date_1");
        put("AP_data",      "select * from gcca.gcca_ariba_alltables_data");
    }};

    public static boolean returnBoolValueIfExists(HashMap<String, String> argMap, String fieldName) {
        return argMap.containsKey(fieldName) && argMap.get(fieldName).toLowerCase().equals("true");
    }

    private static boolean isMonthAbbrev(String s) {
        return monthAbbrevs.contains(s.toUpperCase());
    }

    public static java.util.Date formatDateString(SimpleDateFormat sdf, String dateString) {
        try {
            return sdf.parse(dateString);
        } catch (ParseException e) {
            return null;
        }
    }

    public static Date getDateFromDateString(String dateString) {
        if (dateString != null) {
            Matcher m = genericDatePattern.matcher(dateString);
            if (m.matches()) {
                String f1 = m.group(1), div1 = m.group(2), f2 = m.group(3), div2 = m.group(4), f3 = m.group(5),
                        t       = m.group(6) != null && m.group(7) != null ? (m.group(6).equals("T") ? "'T'" : " ") : null,
                        secStr = (m.group(8) != null)  ? ":ss" : "",
                        msStr  = (m.group(9) != null) ? ".SSS" : "",
                        z      = (m.group(10) != null) ? "'Z'" : "";
                boolean f1isYear = f1.length() == 4,                            // yyyy-MM-dd
                        f2isDay  = f3.length() == 4 && !isMonthAbbrev(f2);      // MM-dd-yyyy
                String format;
                if (f1isYear) {
                    format = "yyyy" + div1 + repeat("M", f2.length()) + div2 + "dd";
                } else if (f2isDay) {
                    format = "MM" + div1 + "dd" + div2 + "yyyy";
                } else {
                    format = "dd" + div1 + repeat("M", f2.length()) + div2 + repeat("y", f3.length());
                }
                if (t != null) {
                    format += t + "hh:mm" + secStr + msStr + z;
                }

                java.util.Date d = formatDateString(new SimpleDateFormat(format), dateString);
                return d != null ? new Date(d.getTime()) : null;
            }
        }
        return null;
    }

    public static void debugPrint(boolean debug, boolean isError, String s) {
        if (!debug) return;
        PrintStream stream = isError ? System.err : System.out;
        stream.println(s);
    }

    private static void debugPrintLine(String key, String val) {
        System.out.printf("   %-15s : %s\n", key, val);
    }

    public static void debugPrintArgs(HashMap<String, String> argMap) {
        boolean debug = argMap.containsKey(CONF_KEY_DEBUG) && argMap.get(CONF_KEY_DEBUG).toLowerCase().equals("true");
        if (!debug) return;

        System.out.println("Command line arguments:");
        for(java.util.Map.Entry<String, String> entry : argMap.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            if (!key.equals(CONF_KEY_FIELDS)) {
                debugPrintLine(key, val);
            }
        }
        List<String> fields = getTokens(argMap.get(CONF_KEY_FIELDS), argMap.get(CONF_KEY_DELIMITER));
        int fieldsPerLine = 3;
        for(int i = 0; i < fields.size(); i += fieldsPerLine) {
            int endIndex = Math.min(i+fieldsPerLine, fields.size());
            System.out.printf("   %-15s %s ", i == 0 ? CONF_KEY_FIELDS : "", i == 0 ? ":" : " ");
            for(int j = i; j < endIndex; j++) {
                System.out.printf("%-30s", fields.get(j));
            }
            System.out.printf("\n");
        }
    }

    public static void checkArgs(HashMap<String, String> argMap) {
        if (!argMap.containsKey(CONF_KEY_CORENAME)) {
            System.err.println("Core name not specified!");
            System.exit(1);
        }
        if (argMap.containsKey(CONF_KEY_ERROR_STRING)) {
            System.err.println("ERROR: " + argMap.get(CONF_KEY_ERROR_STRING));
            System.exit(1);
        }
    }

    public static HashMap<String, String> setMRVariables(String fileName) {
        HashMap<String, String> argMap = new HashMap<String, String>();

        File f = new File(fileName);
        if (!f.exists()) {
            argMap.put(CONF_KEY_ERROR_STRING, "Could not find file " + fileName);
        } else {
            //defaults
            argMap.put(CONF_KEY_DEBUG,     "true");
            argMap.put(CONF_KEY_LOGFILE,   LOG_FILE);
            argMap.put(CONF_KEY_DELIMITER, DEFAULT_DELIMITER);

            try {
                DataInputStream in = new DataInputStream(new FileInputStream(fileName));
                BufferedReader br  = new BufferedReader(new InputStreamReader(in));
                String line;

                while ((line = br.readLine()) != null) {
                    String[] lineComponents = line.split("=");
                    argMap.put(lineComponents[0].toUpperCase(), lineComponents[1]);
                }
                in.close();
            } catch (IOException e) {
                argMap.put(CONF_KEY_ERROR_STRING, e.getMessage());
            }
        }
        checkArgs(argMap);

        if (!argMap.containsKey(CONF_KEY_INPUTFILE_PATH)) {
            argMap.put(CONF_KEY_INPUTFILE_PATH, argMap.get(CONF_KEY_CORENAME) + "_input.tsv");
        }

        String dataInputFileName = new File(argMap.get(CONF_KEY_INPUTFILE_PATH)).getName();
        argMap.put(CONF_KEY_HDFS_INPUTFILE, HDFS_USER_PATH + argMap.get(CONF_KEY_CORENAME) + "/" + dataInputFileName);
        argMap.put(CONF_KEY_SOLR_URL, SOLR_URL_BASE + argMap.get(CONF_KEY_CORENAME));

        debugPrintArgs(argMap);
        return argMap;
    }

    public static String formatColumnObject(Object val, String columnTypeName) {
        if (val == null) {
            return null;
        }

        if (val instanceof Date) {
            return DateField.formatExternal((Date) val);
        }

        if (columnTypeName.equals(SQL_TYPE_STRING) && val instanceof String) {
            Date d = getDateFromDateString(val.toString());
            if (d != null) {
                return DateField.formatExternal(d);
            }
        }
        return val.toString().replaceAll("\\\"", "\\\\\"");
    }

    public static List<String> getTokens(String d, String delimiter) {
        List<String> tokens = new ArrayList<String>();
        if (d.equals("")) return tokens;

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

    public static void writeToFile(String line, FileOutputStream out) throws IOException {
        byte[] allfieldsBytes = line.getBytes();
        byte[] b = new byte[allfieldsBytes.length + newline.length];
        System.arraycopy(allfieldsBytes, 0, b, 0, allfieldsBytes.length);
        System.arraycopy(newline, 0, b, allfieldsBytes.length, newline.length);
        out.write(b);
    }

    public static String join(List<String> list1, String delimiter) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < list1.size(); i++) {
            sb.append(list1.get(i));
            if (i < list1.size() - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static String repeat(String s, int n){
        return new String(new char[n]).replace("\0", s);
    }

    /* JDBC functions
     */
    public static Connection getERPDBConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ERP_DB_USERNAME_KEY, ERP_DB_USERNAME);
        props.setProperty(ERP_DB_PASSWORD_KEY, ERP_DB_PASSWORD);

        try {
            Class.forName(JDBC_DRIVER_CLASS);
            return DriverManager.getConnection(JDBC_SERVER, props);
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    public static ResultSet getJDBCResultSet(String coreName) {
        if (updateStatements.containsKey(coreName)) {
            try {
                Connection conn = getERPDBConnection();
                PreparedStatement preStatement = conn.prepareStatement(updateStatements.get(coreName));

                return preStatement.executeQuery();
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
        }
        return null;
    }

    public static String constructUUIDStringFromRowEntry(List<String> names, List<String> values) {
        String uuidString = "";
        for(int i = 0; i < names.size(); i++) {
            uuidString += names.get(i) + values.get(i);
        }
        return UUID.nameUUIDFromBytes(uuidString.getBytes()).toString();
    }



    /* Solr functions
     */
    private static boolean solrServerUpdateResponseSuccess(UpdateResponse rsp) {
        return rsp.getStatus() == 0;
    }

    public static boolean deleteIndex(SolrServer server)  {
        return deleteByField(server, "*", Arrays.asList("*"));
    }

    public static boolean deleteByField(SolrServer server, String field, List<String> values) {
        try {
            for(String value : values) {
                server.deleteByQuery(field + ":" + value);
            }
            return solrServerUpdateResponseSuccess(server.commit());
        } catch (SolrServerException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return false;
    }

    private static SolrInputDocument createSolrDocument(HashMap<String, Object> fields) {
        SolrInputDocument doc = new SolrInputDocument();
        for(Map.Entry<String, Object> entry : fields.entrySet()) {
            doc.addField(entry.getKey(), entry.getValue());
        }
        return doc;
    }

    public static boolean solrServerUpdate(SolrServer server, List<SolrInputDocument> docs) {
        try {
            UpdateRequest req = new UpdateRequest();
            req.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);

            if (docs != null && docs.size() != 0) {
                req.add(docs);
            }

            if (solrServerUpdateResponseSuccess(req.process(server))) {
                return solrServerUpdateResponseSuccess(server.commit());
            }
        } catch (SolrServerException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return false;
    }

    public static boolean addInfoFiles(String coreName, SolrServer server) {
        HashMap<String, String> hdfsInfoFileContents = getInfoFilesContents(coreName);
        List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        List<String> fieldsToDelete = new ArrayList<String>();

        for(Map.Entry<String, String> entry : hdfsInfoFileContents.entrySet()) {
            String title   = entry.getKey();
            String content = entry.getValue();
            fieldsToDelete.add(title);

            HashMap<String, Object> params = new HashMap<String, Object>();
            params.put(title, content);
            params.put(SOLR_KEY_TITLE, title);
            params.put(SOLR_KEY_ID, UUID.nameUUIDFromBytes(title.getBytes()));
            params.put(SOLR_KEY_URL, title);
            docs.add(createSolrDocument(params));
        }

        deleteByField(server, SOLR_KEY_TITLE, fieldsToDelete);
        return solrServerUpdate(server, docs);
    }

    /* HDFS utility functions
    *
    * */
    public static FileSystem getHDFSFileSystem() throws IOException {
        try {
            return FileSystem.get(URI.create(HDFS_URI), new Configuration(), HDFS_USERNAME);
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    public static boolean addFileToHDFS(String remoteFilePath, String localFilePath) {
        boolean success = false;

        try {
            FileSystem fs = getHDFSFileSystem();
            Path srcPath = new Path(localFilePath);
            Path dstPath = new Path(remoteFilePath);

            if (fs.exists(dstPath)) {
                fs.delete(dstPath, false);
            }
            fs.copyFromLocalFile(srcPath, dstPath);
            success = fs.exists(dstPath);
            fs.close();

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return success;
    }

    public static HashMap<String, String> getInfoFilesContents(String coreName) {
        HashMap<String, String> infoFileContents = new HashMap<String, String>();

        Path fieldsCustomFile = new Path(HDFS_URI + HDFS_USER_PATH + coreName, HDFS_FIELDS_FILENAME);
        String contents = new String(getFileContentsAsBytes(fieldsCustomFile));

        if (!contents.equals("")) {
            for(String fieldString : contents.split("\n")) {
                String[] fieldDataArray = fieldString.split("=");
                if (fieldDataArray.length == 2) {
                    infoFileContents.put(fieldDataArray[0], fieldDataArray[1]);
                }
            }
        }
        return infoFileContents;
    }

    public static byte[] getFileContentsAsBytes(Path remoteFilePath) {
        try {
            FileSystem fs = getHDFSFileSystem();

            if (!fs.exists(remoteFilePath)) {
                System.err.println("File " + remoteFilePath.toString() + " does not exist on HDFS. ");
                return new byte[0];
            }

            long length = fs.getContentSummary(remoteFilePath).getLength();
            byte[] contents = new byte[(int)length];

            if (length > Integer.MAX_VALUE) {
                System.err.println("File " + remoteFilePath.toString() + " is too large. ");
                return new byte[0];
            }

            DataInputStream d = new DataInputStream(fs.open(remoteFilePath));
            int offset = 0, numRead = 0;
            while (offset < contents.length && (numRead = d.read(contents, offset, contents.length-offset)) >= 0) {
                offset += numRead;
            }

            if (offset < contents.length) {
                throw new IOException("Could not completely read file " + remoteFilePath);
            }

            d.close();
            return contents;

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return new byte[0];
    }
}
