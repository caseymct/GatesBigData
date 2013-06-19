package GatesBigData.constants;

import GatesBigData.utils.Utils;
import org.apache.nutch.net.URLNormalizers;

import java.net.URI;
import java.util.Arrays;
import java.util.regex.Pattern;

public class HDFS extends Constants {
    public static final int HDFS_PORT              = 8020;
    public static final String HDFS_SERVER         = "denlx010.dn.gates.com";
    public static final String HDFS_URI_STRING     = Utils.constructAddress(HDFS_PROTOCOL, HDFS_SERVER, HDFS_PORT);
    public static final URI HDFS_URI               = URI.create(HDFS_URI_STRING);
    public static final String COLLECTIONINFO_FILE = "collectioninfo.json";
    public static final String USERNAME            = "hdfs";

    public static final String USER_DIR_URI        = Utils.constructAddress(HDFS_URI_STRING, Arrays.asList("user", USERNAME));
    public static final String USER_DIR            = "/user/" + USERNAME;
    public static final String CRAWL_DIR           = "crawl";
    public static final String SEGMENTS_DIR        = "segments";
    public static final String CRAWLDB_DIR         = URLNormalizers.SCOPE_CRAWLDB;
    public static final String DATA_DIR            = "data";
    public static final String INDEX_DIR           = "index";
    public static Pattern MATCH_ALL_PATTERN        = Pattern.compile(".*");

    public static String stripHDFSURI(String s) {
        return s.startsWith(HDFS_URI_STRING) ? s.substring(HDFS_URI_STRING.length()) : s;
    }
}
