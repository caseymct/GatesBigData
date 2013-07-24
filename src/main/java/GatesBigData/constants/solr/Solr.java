package GatesBigData.constants.solr;

import GatesBigData.constants.Constants;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

import static GatesBigData.utils.URLUtils.*;

public class Solr extends Constants {
    public static final int SOLR_PORT                   = 8983;
    public static final int ZOOKEEPER_PORT              = 2181;

    public static final int ZK_CLIENT_TIMEOUT           = 5000;
    public static final int ZK_CONNECT_TIMEOUT          = 5000;
    
    public static final String SOLR_ENDPOINT            = "solr";
    public static final String ZK_ENDPOINT              = "zookeeper";
    public static final String LUKE_ENDPOINT            = "admin/luke";
    public static final String ZK_CLUSTERSTATE_FILE     = "clusterstate.json";
    public static final String ZK_NODE                  = "znode";
    public static final String DATA_NODE                = "data";
    public static final String SOLR_SCHEMA_FILE         = "schema.xml";

    public static final String PRODUCTION_SOLR_SERVER_1 = "denlx011.dn.gates.com";
    public static final String PRODUCTION_SOLR_SERVER_2 = "denlx012.dn.gates.com";
    public static final String PRODUCTION_SOLR_SERVER_3 = "denlx013.dn.gates.com";
    public static final String PRODUCTION_SOLR_SERVER_4 = "denlx014.dn.gates.com";

    public static final String PRODUCTION_ZK_SERVER     = "denlx010.dn.gates.com";
    
    public static final List<String> CLOUD_SOLR_SERVERS = Arrays.asList(
            constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_1, SOLR_PORT, SOLR_ENDPOINT),
            constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_2, SOLR_PORT, SOLR_ENDPOINT),
            constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_3, SOLR_PORT, SOLR_ENDPOINT),
            constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_4, SOLR_PORT, SOLR_ENDPOINT));

    // Solr core name constants
    public static final String THUMBNAILS_COLLECTION_NAME = "thumbnails";
    public static final String SUGGESTIONS_COLLECTION_KEY = "suggestions";
    public static boolean skip(String collection) {
        return collection.equals(THUMBNAILS_COLLECTION_NAME) || collection.contains(SUGGESTIONS_COLLECTION_KEY);
    }

    public static final String NULL_STRING              = "null";
    public static final String VIEW_TYPE_PREVIEW        = "preview";
    public static final String VIEW_TYPE_AUDITVIEW      = "auditview";

    public static final String[] CHARS_TO_ENCODE   = {  "-", "\\+", "\\[", "\\]", "\\(", "\\)",   ":"};
    public static final String[] CHARS_ENCODED_VAL = {"%96", "%2B", "%5B", "%5D", "%28", "%29", "%3A"};

    public static boolean hasCharsToEncode(String word) {
        return word.matches(".*(" + StringUtils.join(CHARS_TO_ENCODE, "|") + ").*");
    }

    public static String escape(String word) {
        for(int i = 0; i < CHARS_TO_ENCODE.length; i++) {
            word = word.replaceAll(CHARS_TO_ENCODE[i], CHARS_ENCODED_VAL[i]);
        }
        return word;
    }
}
