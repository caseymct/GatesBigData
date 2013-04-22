package GatesBigData.utils;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.UpdateResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public class Constants {

    public static boolean USE_PRODUCTION_SOLR_SERVER = false;

    // Invalid numbers
    public static int INVALID_INTEGER               = -99999;
    public static long INVALID_LONG                 = -99999;
    public static double INVALID_DOUBLE             = -99999.999999;

    public static final String DEFAULT_NEWLINE      = System.getProperty("line.separator");
    public static final String DEFAULT_DELIMETER    = ",";

    public static final String UTF8                 = "UTF-8";
    public static final String CHARSET_ENC_KEY      = "charset";
    public static final String JSON_CONTENT_TYPE    = "application/json";
    public static final String CSV_CONTENT_TYPE     = "text/csv";
    public static final String ZIP_CONTENT_TYPE     = "application/zip";
    public static final String TEXT_CONTENT_TYPE    = "text/plain";
    public static final String XML_CONTENT_TYPE     = "text/xml";
    public static final String FLASH_CONTENT_TYPE   = "application/x-shockwave-flash";
    public static final String IMG_CONTENT_TYPE     = "image/png";
    public static final String BASE64_CONTENT_TYPE  = "data:image/png;base64";

    public static final String SWF_FILE_EXT         = "swf";
    public static final String IMG_FILE_EXT         = "png";
    public static final String TEXT_FILE_EXT        = "txt";
    public static final String JSON_FILE_EXT        = "json";
    public static final String PDF_FILE_EXT         = "pdf";
    public static final String CSV_FILE_EXT         = "csv";
    public static final String ZIP_FILE_EXT         = "zip";

    public static final String CONTENT_TYPE_HEADER      = "Content-Type";
    public static final String CONTENT_LENGTH_HEADER    = "Content-Length";
    public static final String CONTENT_DISP_HEADER      = "Content-Disposition";
    public static final String CONTENT_TYPE_VALUE       = JSON_CONTENT_TYPE + "; " + CHARSET_ENC_KEY + "=" + UTF8;

    public static final int SOLR_PORT                   = 8984;
    public static final int ZOOKEEPER_PORT              = 2181;
    public static final int ERP_DB_PORT                 = 1591;
    public static final int ZK_CLIENT_TIMEOUT           = 5000;
    public static final int ZK_CONNECT_TIMEOUT          = 5000;

    public static final String HTTP_PROTOCOL            = "http://";
    public static final String JDBC_PROTOCOL            = "jdbc:";
    public static final String SMB_PROTOCOL             = "smb://";

    public static final String JDBC_DB_TYPE             = "oracle";
    public static final String JDBC_DRIVER_TYPE         = "thin";
    public static final String ERP_DB_NAME              = "TNS_ADMIN";
    public static final String ERP_DB_SID               = "ERPDBA";
    public static final String ERP_DB_USERNAME          = "Look";
    public static final String ERP_DB_PASSWORD          = "look";
    public static final String ERP_DB_USERNAME_KEY      = "user";
    public static final String ERP_DB_PASSWORD_KEY      = "password";
    public static final String JDBC_DRIVER_CLASS        = "oracle.jdbc.OracleDriver";

    public static final String SMB_DOMAIN               = "NA";
    public static final String SMB_USERNAME             = "bigdatasvc";
    public static final String SMB_PASSWORD             = "Crawl2013";

    public static final String ERP_DB_SERVER            = "erpdbadb.dn.gates.com";
    public static final String PRODUCTION_SERVER        = "denlx006.dn.gates.com";
    public static final String LOCAL_SOLR_SERVER        = HTTP_PROTOCOL + "localhost:" + SOLR_PORT;
    public static final String PRODUCTION_SOLR_SERVER   = HTTP_PROTOCOL + PRODUCTION_SERVER + ":" + SOLR_PORT;
    public static final String SOLR_SERVER              = USE_PRODUCTION_SOLR_SERVER ? PRODUCTION_SOLR_SERVER : LOCAL_SOLR_SERVER;
    public static final String ZOOKEEPER_SERVER         = PRODUCTION_SERVER + ":" + ZOOKEEPER_PORT;
    public static final String JDBC_SERVER              = JDBC_PROTOCOL + JDBC_DB_TYPE + ":" + JDBC_DRIVER_TYPE + ":@//" + ERP_DB_SERVER + ":" + ERP_DB_PORT + "/" + ERP_DB_SID;

    public static final String SQL_TYPE_STRING          = "VARCHAR2";

    public static final int SOLR_RESPONSE_CODE_SUCCESS  = 0;
    public static final int SOLR_RESPONSE_CODE_ERROR    = -1;

    // Solr core name constants
    public static final String SOLR_THUMBNAILS_CORE_NAME            = "thumbnails";
    public static final String SOLR_SUGGEST_CORE_SUFFIX             = "_suggestions";
    public static final String SOLR_TEST_CORE_SUFFIX                = "_test";

    // Solr schema file default
    public static final String SOLR_SCHEMA_DEFAULT_FILE_NAME        = "schema.xml";

    // Solr field name constants
    public static final String SOLR_FIELD_TYPE_PREFIX_SUFFIX        = "Prefix";
    public static final String SOLR_FIELD_TYPE_FACET_SUFFIX         = ".facet";
    public static final String SOLR_FIELD_NAME_THUMBNAIL            = "thumbnail";
    public static final String SOLR_FIELD_NAME_CORE_TITLE           = "coreTitle";
    public static final String SOLR_FIELD_NAME_STRUCTURED_DATA      = "structuredData";
    public static final String SOLR_FIELD_NAME_SUGGESTION_CORE      = "suggestionCore";
    public static final String SOLR_FIELD_NAME_CONTENT              = "content";
    public static final String SOLR_FIELD_NAME_TIMESTAMP            = "timestamp";
    public static final String SOLR_FIELD_NAME_HDFSKEY              = "HDFSKey";
    public static final String SOLR_FIELD_NAME_HDFSSEGMENT          = "HDFSSegment";
    public static final String SOLR_FIELD_NAME_BOOST                = "boost";
    public static final String SOLR_FIELD_NAME_VERSION              = "_version_";
    public static final String SOLR_FIELD_NAME_CONTENT_TYPE         = "content_type";
    public static final String SOLR_FIELD_NAME_ID                   = "id";
    public static final String SOLR_FIELD_NAME_CORE                 = "core";
    public static final String SOLR_FIELD_NAME_URL                  = "url";
    public static final String SOLR_FIELD_NAME_SCORE                = "score";
    public static final String SOLR_FIELD_NAME_COUNT                = "count";
    public static final String SOLR_FIELD_NAME_TITLE                = "title";
    public static final String SOLR_FIELD_NAME_LAST_MODIFIED        = "last_modified";
    public static final String SOLR_FIELD_NAME_CREATE_DATE          = "creation_date";
    public static final String SOLR_FIELD_NAME_LAST_AUTHOR          = "last_author";
    public static final String SOLR_FIELD_NAME_APPLICATION_NAME     = "application_name";
    public static final String SOLR_FIELD_NAME_AUTHOR               = "author";
    public static final String SOLR_FIELD_NAME_COMPANY              = "company";
    public static final String SOLR_FIELD_NAME_FACET_FIELDS         = "FACETFIELDS";
    public static final String SOLR_FIELD_NAME_PREVIEW_FIELDS       = "PREVIEWFIELDS";
    public static final String SOLR_FIELD_NAME_AUDIT_FIELDS         = "AUDITFIELDS";
    public static final String SOLR_FIELD_NAME_TABLE_FIELDS         = "TABLEFIELDS";
    public static final String SOLR_FIELD_NAME_WORDTREE_FIELDS      = "WORDTREEFIELDS";
    public static final String SOLR_FIELD_NAME_X_AXIS_FIELDS        = "XAXIS";
    public static final String SOLR_FIELD_NAME_Y_AXIS_FIELDS        = "YAXIS";
    public static final String SOLR_FIELD_NAME_SERIES_FIELDS        = "SERIES";
    public static List<String> SOLR_INFO_FIELD_NAMES =
            Arrays.asList(SOLR_FIELD_NAME_WORDTREE_FIELDS, SOLR_FIELD_NAME_AUDIT_FIELDS, SOLR_FIELD_NAME_PREVIEW_FIELDS,
                          SOLR_FIELD_NAME_TABLE_FIELDS,    SOLR_FIELD_NAME_FACET_FIELDS, SOLR_FIELD_NAME_X_AXIS_FIELDS,
                          SOLR_FIELD_NAME_Y_AXIS_FIELDS,   SOLR_FIELD_NAME_SERIES_FIELDS);

    public static final String SOLR_FIELD_TYPE_DATE                 = "date";
    public static final String SOLR_FIELD_TYPE_STRING               = "string";
    public static final String SOLR_FIELD_TYPE_INT                  = "int";
    public static final String SOLR_FIELD_TYPE_LONG                 = "long";
    public static final String SOLR_FIELD_TYPE_FLOAT                = "float";
    public static final String SOLR_FIELD_TYPE_DOUBLE               = "double";
    public static final String SOLR_FIELD_TYPE_TEXT_GENERAL         = "text_general";
    public static final List<String> SOLR_NUMBER_FIELDS             = Arrays.asList(SOLR_FIELD_TYPE_INT, SOLR_FIELD_TYPE_FLOAT, SOLR_FIELD_TYPE_LONG, SOLR_FIELD_TYPE_DOUBLE);
    public static final List<String> SOLR_TEXT_FIELDS               = Arrays.asList(SOLR_FIELD_TYPE_STRING, SOLR_FIELD_TYPE_TEXT_GENERAL);
    // Solr query constants
    public static final String SOLR_PARAM_QUERY                     = "q";
    public static final String SOLR_PARAM_FIELDLIST                 = "fl";
    public static final String SOLR_PARAM_FILE                      = "file";
    public static final String SOLR_PARAM_WT                        = "wt";
    public static final String SOLR_PARAM_HIGHLIGHT                 = "hl";
    public static final String SOLR_PARAM_HIGHLIGHT_QUERY           = SOLR_PARAM_HIGHLIGHT + ".q";
    public static final String SOLR_PARAM_HIGHLIGHT_FRAGSIZE        = SOLR_PARAM_HIGHLIGHT + ".fragsize";
    public static final String SOLR_PARAM_HIGHLIGHT_PRE             = SOLR_PARAM_HIGHLIGHT + ".simple.pre";
    public static final String SOLR_PARAM_HIGHLIGHT_POST            = SOLR_PARAM_HIGHLIGHT + ".simple.post";
    public static final String SOLR_PARAM_HIGHLIGHT_SNIPPETS        = SOLR_PARAM_HIGHLIGHT + ".snippets";
    public static final String SOLR_PARAM_HIGHLIGHT_FIELDLIST       = SOLR_PARAM_HIGHLIGHT + ".fl";
    public static final String SOLR_PARAM_FACET_DATE_FIELD          = "facet.date";
    public static final String SOLR_PARAM_FACET_DATE_START          = SOLR_PARAM_FACET_DATE_FIELD + ".start";
    public static final String SOLR_PARAM_FACET_DATE_END            = SOLR_PARAM_FACET_DATE_FIELD + ".end";
    public static final String SOLR_PARAM_FACET_DATE_GAP            = SOLR_PARAM_FACET_DATE_FIELD + ".gap";
    public static final String SOLR_PARAM_GROUP                     = "group";
    public static final String SOLR_PARAM_GROUP_LIMIT               = SOLR_PARAM_GROUP + ".limit";
    public static final String SOLR_PARAM_GROUP_FIELD               = SOLR_PARAM_GROUP + ".field";
    public static final String SOLR_PARAM_GROUP_SORT                = SOLR_PARAM_GROUP + ".sort";

    public static final int SOLR_DEFAULT_VALUE_START                    = 0;
    public static final int SOLR_DEFAULT_VALUE_ROWS                     = 10;
    public static final int SOLR_DEFAULT_VALUE_HIGHLIGHT_SNIPPETS       = 5;
    public static final int SOLR_DEFAULT_VALUE_HIGHLIGHT_FRAGSIZE       = 50;

    public static final String SOLR_DEFAULT_VALUE_QUERY                 = "*:*";
    public static final String SOLR_DEFAULT_VALUE_WT                    = "json";
    public static final String SOLR_DEFAULT_VALUE_SORT_FIELD            = "score";
    public static final String SOLR_DEFAULT_VALUE_HIGHLIGHT_PRE         = "<span class='highlight_text'>";
    public static final String SOLR_DEFAULT_VALUE_HIGHLIGHT_POST        = "</span>";
    public static final String SOLR_DEFAULT_VALUE_GROUP_SORT            = SOLR_FIELD_NAME_SCORE + " desc";
    public static final SolrQuery.ORDER SOLR_DEFAULT_VALUE_SORT_ORDER   = SolrQuery.ORDER.asc;

    //solr response constants
    public static final String SOLR_RESPONSE_HIGHLIGHTING_KEY           = "highlighting";
    public static final String SOLR_RESPONSE_KEY                        = "response";
    public static final String SOLR_RESPONSE_HEADER_PARAMS_KEY          = "params";
    public static final String SOLR_RESPONSE_FACET_KEY                  = "facet_counts";
    public static final String SOLR_RESPONSE_NAME_KEY                   = "name";
    public static final String SOLR_RESPONSE_VALUES_KEY                 = "values";

    // Group limit for suggestions. This is how many results are returned per group
    public static final int SOLR_GROUP_LIMIT_UNLIMITED                  = -1;
    public static final int SOLR_SUGGESTION_GROUP_LIMIT                 = 1;
    public static final int SOLR_ANALYSIS_GROUP_LIMIT                   = 0;  // return 0 results per group (for speed)
    public static final int SOLR_ANALYSIS_ROWS_LIMIT                    = SOLR_GROUP_LIMIT_UNLIMITED; // return all groups


    public static final String SEARCH_RESPONSE_NUM_FOUND_KEY            = "num_found";
    public static final String SEARCH_RESPONSE_NUM_DOCS_KEY             = "docs";

    public static final String SUGGESTION_RESPONSE_KEY                  = "suggestions";

    public static final String SOLR_BOOLEAN_INTERSECTION                = "OR";
    public static final String SOLR_BOOLEAN_UNION                       = "AND";
    public static final String SOLR_BOOLEAN_DEFAULT                     = SOLR_BOOLEAN_INTERSECTION;

    public static final int SOLR_OPERATION_ADD           = 1;
    public static final int SOLR_OPERATION_UPDATE        = 2;
    public static final int SOLR_OPERATION_DELETE        = 3;
    public static final int SOLR_OPERATION_ADD_INFOFILES = 4;
    public static final HashMap<Integer, String> SOLR_OPERATION_MSGS = new HashMap<Integer, String>() {{
        put(SOLR_OPERATION_ADD,           "Add to index");
        put(SOLR_OPERATION_DELETE,        "Delete index");
        put(SOLR_OPERATION_UPDATE,        "Update index");
        put(SOLR_OPERATION_ADD_INFOFILES, "Add info files");
    }};

    public static final String VIEW_TYPE_PREVIEW                        = "preview";
    public static final String VIEW_TYPE_FULLVIEW                       = "fullview";
    public static final String VIEW_TYPE_AUDITVIEW                      = "auditview";

    public static Pattern VIEW_TYPE_PATTERN = Pattern.compile(VIEW_TYPE_PREVIEW + "|" + VIEW_TYPE_AUDITVIEW + "|" + VIEW_TYPE_FULLVIEW);

    public static final HashMap<String, String> VIEW_TYPE_TO_INFO_FIELD_MAP = new HashMap<String, String>() {{
        put(VIEW_TYPE_PREVIEW, SOLR_FIELD_NAME_PREVIEW_FIELDS);
        put(VIEW_TYPE_AUDITVIEW, SOLR_FIELD_NAME_AUDIT_FIELDS);
        put(VIEW_TYPE_FULLVIEW, VIEW_TYPE_FULLVIEW);
    }};

    public static Pattern MATCH_ALL_PATTERN        = Pattern.compile(".*");

    public static String getContentDispositionFileAttachHeader(String fileName) {
        return "attachment; fileName=" + fileName;
    }

    public static boolean SolrResponseSuccess(UpdateResponse rsp) {
        return rsp.getStatus() == SOLR_RESPONSE_CODE_SUCCESS;
    }

    public static boolean SolrResponseSuccess(int code) {
        return code == SOLR_RESPONSE_CODE_SUCCESS;
    }

    public static String SOLR_FACET_DATE_START(String fieldName) {
        return "f." + fieldName + "." + SOLR_PARAM_FACET_DATE_START;
    }

    public static String SOLR_FACET_DATE_END(String fieldName) {
        return "f." + fieldName + "." + SOLR_PARAM_FACET_DATE_END;
    }

    public static String SOLR_FACET_DATE_GAP(String fieldName) {
        return "f." + fieldName + "." + SOLR_PARAM_FACET_DATE_GAP;
    }
}
