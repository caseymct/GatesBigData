package GatesBigData.constants.solr;

import GatesBigData.constants.Constants;
import GatesBigData.utils.Utils;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

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
    public static final String SOLR_SCHEMA_FILE         = "schema.xml";

    public static final String PRODUCTION_SOLR_SERVER_1 = "denlx011.dn.gates.com";
    public static final String PRODUCTION_SOLR_SERVER_2 = "denlx012.dn.gates.com";
    public static final String PRODUCTION_SOLR_SERVER_3 = "denlx013.dn.gates.com";
    public static final String PRODUCTION_SOLR_SERVER_4 = "denlx014.dn.gates.com";

    public static final String PRODUCTION_ZK_SERVER_1   = "denlx010.dn.gates.com";
    public static final String PRODUCTION_ZK_SERVER_2   = "denlx011.dn.gates.com";
    public static final String PRODUCTION_ZK_SERVER_3   = "denlx012.dn.gates.com";
    
    public static final String LOCAL_SOLR_SERVER        = Utils.constructAddress(HTTP_PROTOCOL, LOCALHOST_SERVER, SOLR_PORT, SOLR_ENDPOINT);
    public static final String PRODUCTION_SOLR_SERVER   = Utils.constructAddress(Constants.HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_1, SOLR_PORT, SOLR_ENDPOINT);
    public static final String SOLR_SERVER              = USE_PRODUCTION_SOLR_SERVER ? PRODUCTION_SOLR_SERVER : LOCAL_SOLR_SERVER;
    public static final String SOLR_SERVER_ZK_ENDPOINT  = Utils.constructAddress(SOLR_SERVER, ZK_ENDPOINT);
    public static final String ZOOKEEPER_SERVER         = Utils.constructAddress(HTTP_PROTOCOL, PRODUCTION_ZK_SERVER_1, ZOOKEEPER_PORT);
    
    public static final List<String> CLOUD_SOLR_SERVERS = Arrays.asList(
            Utils.constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_1, SOLR_PORT, SOLR_ENDPOINT),
            Utils.constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_2, SOLR_PORT, SOLR_ENDPOINT),
            Utils.constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_3, SOLR_PORT, SOLR_ENDPOINT),
            Utils.constructAddress(HTTP_PROTOCOL, PRODUCTION_SOLR_SERVER_4, SOLR_PORT, SOLR_ENDPOINT));

    // Solr core name constants
    public static final String THUMBNAILS_COLLECTION_NAME = "thumbnails";
    public static final String SUGGESTIONS_COLLECTION_KEY = "suggestions";
    public static boolean skip(String collection) {
        return  collection.equals(THUMBNAILS_COLLECTION_NAME) ||
                collection.contains(SUGGESTIONS_COLLECTION_KEY);
    }

    public static final String NULL_STRING              = "null";
    public static final String VIEW_TYPE_PREVIEW        = "preview";
    public static final String VIEW_TYPE_FULLVIEW       = "fullview";
    public static final String VIEW_TYPE_AUDITVIEW      = "auditview";
    public static final Pattern VIEW_TYPE_PATTERN       = Pattern.compile(VIEW_TYPE_PREVIEW + "|" + VIEW_TYPE_AUDITVIEW + "|" + VIEW_TYPE_FULLVIEW);

    public static boolean isAuditViewOrPreview(String viewType) {
        return viewType.equals(VIEW_TYPE_AUDITVIEW) || viewType.equals(VIEW_TYPE_PREVIEW);
    }

    public static String getViewType(String viewType) {
        return viewType != null && VIEW_TYPE_PATTERN.matcher(viewType).matches() ? viewType : VIEW_TYPE_PREVIEW;
    }

    public static String getInfoField(String viewType) {
        if (viewType.equals(VIEW_TYPE_PREVIEW)) {
            return FieldNames.PREVIEW_FIELDS;
        } else if (viewType.equals(VIEW_TYPE_AUDITVIEW)) {
            return FieldNames.AUDIT_FIELDS;
        } else if (viewType.equals(VIEW_TYPE_FULLVIEW)) {
            return VIEW_TYPE_FULLVIEW;
        }
        return NULL_STRING;
    }

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
