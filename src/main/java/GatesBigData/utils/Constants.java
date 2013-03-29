package GatesBigData.utils;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.schema.SchemaField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public class Constants {

    public static int INVALID_INTEGER               = -99999;
    public static long INVALID_LONG                 = -99999;
    public static double INVALID_DOUBLE             = -99999.999999;

    public static final String DEFAULT_NEWLINE      = System.getProperty("line.separator");
    public static final String DEFAULT_DELIMETER    = ",";

    public static final String HTTP_PROTOCOL        = "http://";
    public static final String PRODUCTION_SERVER    = "denlx006.dn.gates.com";
    public static final String PRODUCTION_HOSTNAME  = "denlx006";
    public static final String ERP_DB_HOSTNAME      = "erpdbadb.dn.gates.com";

    public static final String ERP_DB_SID           = "ERPDBA";
    public static final String SMB_PROTOCOL_STRING  = "smb://";
    public static final String SMB_DOMAIN           = "NA";
    public static final String SMB_USERNAME         = "bigdatasvc";
    public static final String SMB_PASSWORD         = "Crawl2013";

    public static final int SOLR_PORT               = 8984;
    public static final int ZOOKEEPER_PORT          = 2181;
    public static final int ERP_DB_PORT             = 1591;
    public static final String SOLR_SERVER          = HTTP_PROTOCOL + PRODUCTION_SERVER + ":" + SOLR_PORT;
    public static final String ZOOKEEPER_SERVER     = PRODUCTION_SERVER + ":" + ZOOKEEPER_PORT;
    public static final int ZK_CLIENT_TIMEOUT       = 5000;
    public static final int ZK_CONNECT_TIMEOUT      = 5000;

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

    public static final String CONTENT_TYPE_HEADER   = "Content-Type";
    public static final String CONTENT_LENGTH_HEADER = "Content-Length";
    public static final String CONTENT_DISP_HEADER   = "Content-Disposition";
    public static final String CONTENT_TYPE_VALUE    = JSON_CONTENT_TYPE + "; " + CHARSET_ENC_KEY + "=" + UTF8;

    // Solr core name constants
    public static final String SOLR_THUMBNAILS_CORE_NAME         = "thumbnails";
    public static final String SOLR_SUGGEST_CORE_SUFFIX          = "_suggestions";

    // Solr schema file default
    public static final String SOLR_SCHEMA_DEFAULT_FILE_NAME     = "schema.xml";

    // Solr field name constants
    public static final String SOLR_THUMBNAIL_FIELD_NAME         = "thumbnail";
    public static final String SOLR_CORE_TITLE_FIELD_NAME        = "coreTitle";
    public static final String SOLR_STRUCTURED_DATA_FIELD_NAME   = "structuredData";
    public static final String SOLR_SUGGESTION_CORE_FIELD_NAME   = "suggestionCore";
    public static final String SOLR_CONTENT_FIELD_NAME           = "content";
    public static final String SOLR_TIMESTAMP_FIELD_NAME         = "timestamp";
    public static final String SOLR_VERSION_FIELD_NAME           = "_version_";
    public static final String SOLR_CONTENT_TYPE_FIELD_NAME      = "content_type";
    public static final String SOLR_ID_FIELD_NAME                = "id";
    public static final String SOLR_CORE_FIELD_NAME              = "core";
    public static final String SOLR_URL_FIELD_NAME               = "url";
    public static final String SOLR_SCORE_FIELD_NAME             = "score";
    public static final String SOLR_COUNT_FIELD_NAME             = "count";
    public static final String SOLR_TITLE_FIELD_NAME             = "title";
    public static final String SOLR_FACET_FIELD_SUFFIX           = ".facet";
    public static final String SOLR_FACET_FIELDS_ID_FIELD_NAME   = "FACETFIELDS";
    public static final String SOLR_PREVIEW_FIELDS_ID_FIELD_NAME = "PREVIEWFIELDS";
    public static final String SOLR_AUDIT_FIELDS_ID_FIELD_NAME   = "AUDITFIELDS";
    public static final String SOLR_TABLE_FIELDS_ID_FIELD_NAME   = "TABLEFIELDS";
    public static final String SOLR_WORDTREE_FIELDS_ID_FIELD_NAME = "WORDTREEFIELDS";
    public static final String SOLR_XAXIS_FIELDS_ID_FIELD_NAME   = "XAXIS";
    public static final String SOLR_YAXIS_FIELDS_ID_FIELD_NAME   = "YAXIS";
    public static final String SOLR_SERIES_FIELDS_ID_FIELD_NAME  = "SERIES";
    public static List<String> SOLR_ALL_INFO_FIELDS = Arrays.asList(SOLR_WORDTREE_FIELDS_ID_FIELD_NAME,
            SOLR_AUDIT_FIELDS_ID_FIELD_NAME, SOLR_PREVIEW_FIELDS_ID_FIELD_NAME, SOLR_TABLE_FIELDS_ID_FIELD_NAME,
            SOLR_FACET_FIELDS_ID_FIELD_NAME, SOLR_XAXIS_FIELDS_ID_FIELD_NAME, SOLR_YAXIS_FIELDS_ID_FIELD_NAME,
            SOLR_SERIES_FIELDS_ID_FIELD_NAME);

    public static final String SOLR_FIELD_TYPE_DATE              = "date";
    public static final String SOLR_FIELD_TYPE_STRING            = "string";
    public static final String SOLR_FIELD_TYPE_INT               = "int";
    public static final String SOLR_FIELD_TYPE_LONG              = "long";
    public static final String SOLR_FIELD_TYPE_FLOAT             = "float";

    // Solr query constants
    public static final String SOLR_QUERY_PARAM                 = "q";
    public static final String SOLR_QUERY_DEFAULT               = "*:*";
    public static final String SOLR_FILTERQUERY_PARAM           = "fq";
    public static final String SOLR_FIELDLIST_PARAM             = "fl";
    public static final String SOLR_FILE_PARAM                  = "file";
    public static final String SOLR_WT_PARAM                    = "wt";
    public static final String SOLR_WT_DEFAULT                  = "json";
    public static final int SOLR_START_DEFAULT                  = 0;
    public static final int SOLR_ROWS_DEFAULT                   = 10;
    public static final String SOLR_HIGHLIGHT_PARAM             = "hl";
    public static final String SOLR_HIGHLIGHT_QUERY_PARAM       = SOLR_HIGHLIGHT_PARAM + ".q";
    public static final String SOLR_HIGHLIGHT_FRAGSIZE_PARAM    = SOLR_HIGHLIGHT_PARAM + ".fragsize";
    public static final String SOLR_HIGHLIGHT_PRE_PARAM         = SOLR_HIGHLIGHT_PARAM + ".simple.pre";
    public static final String SOLR_HIGHLIGHT_PRE_DEFAULT       = "<span class='highlight_text'>";
    public static final String SOLR_HIGHLIGHT_POST_PARAM        = SOLR_HIGHLIGHT_PARAM + ".simple.post";
    public static final String SOLR_HIGHLIGHT_POST_DEFAULT      = "</span>";
    public static final String SOLR_HIGHLIGHT_SNIPPETS_PARAM    = SOLR_HIGHLIGHT_PARAM + ".snippets";
    public static final int SOLR_HIGHLIGHT_SNIPPETS_DEFAULT     = 5;
    public static final int SOLR_HIGHLIGHT_FRAGSIZE_DEFAULT     = 50;
    public static final String SOLR_HIGHLIGHT_FIELDLIST_PARAM   = SOLR_HIGHLIGHT_PARAM + ".fl";
    public static final String SOLR_SORT_PARAM                  = "sort";
    public static final String SOLR_SORT_FIELD_DEFAULT          = "score";
    public static final String SOLR_FACET_PARAM                 = "facet";
    public static final String SOLR_FACET_DEFAULT               = "true";
    public static final String SOLR_FACET_MISSING_PARAM         = "facet.missing";
    public static final String SOLR_FACET_MISSING_DEFAULT       = "true";
    public static final String SOLR_FACET_LIMIT_PARAM           = "facet.limit";
    public static final String SOLR_FACET_FIELD_PARAM           = "facet.field";
    public static final String SOLR_FACET_DATE_FIELD_PARAM      = "facet.date";
    public static final String SOLR_FACET_DATE_START_PARAM      = "facet.date.start";
    public static final String SOLR_FACET_DATE_END_PARAM        = "facet.date.end";
    public static final String SOLR_FACET_DATE_GAP_PARAM        = "facet.date.gap";
    public static final String SOLR_GROUP_PARAM                 = "group";
    public static final String SOLR_GROUP_LIMIT_PARAM           = "group.limit";
    public static final String SOLR_GROUP_LIMIT_DEFAULT         = "10";
    public static final String SOLR_GROUP_LIMIT_UNLIMITED       = "-1";
    public static final String SOLR_GROUP_FIELD_PARAM           = "group.field";
    public static final String SOLR_GROUP_SORT_PARAM            = "group.sort";
    public static final String SOLR_GROUP_SORT_DEFAULT          = "score desc";
    public static final SolrQuery.ORDER SOLR_SORT_ORDER_DEFAULT = SolrQuery.ORDER.asc;

    //solr response constants
    public static final String SOLR_RESPONSE_HIGHLIGHTING_KEY   = "highlighting";
    public static final String SOLR_RESPONSE_KEY                = "response";
    public static final String SOLR_RESPONSE_HEADER_PARAMS_KEY  = "params";
    public static final String SOLR_RESPONSE_FACET_KEY          = "facet_counts";
    public static final String SOLR_RESPONSE_FACET_FIELDS_KEY   = "facet_fields";
    public static final String SOLR_RESPONSE_FACET_DATES_KEY    = "facet_dates";

    // Group limit for suggestions. This is how many results are returned per group
    public static final int SOLR_SUGGESTION_GROUP_LIMIT         = 1;
    public static final int SOLR_ANALYSIS_GROUP_LIMIT           = 0;  // return 0 results per group (for speed)
    public static final int SOLR_ANALYSIS_ROWS_LIMIT            = -1; // return all groups

    public static final String SEARCH_RESPONSE_NUM_FOUND_KEY    = "num_found";
    public static final String SEARCH_RESPONSE_NUM_DOCS_KEY     = "docs";

    public static final String SUGGESTION_RESPONSE_KEY          = "suggestions";

    public static final String SOLR_INTERSECTION_BOOLEAN_OP = "OR";
    public static final String SOLR_UNION_BOOLEAN_OP        = "AND";
    public static final String SOLR_DEFAULT_BOOLEAN_OP      = SOLR_INTERSECTION_BOOLEAN_OP;

    public static final String VIEW_TYPE_PREVIEW            = "preview";
    public static final String VIEW_TYPE_FULLVIEW           = "fullview";
    public static final String VIEW_TYPE_AUDITVIEW          = "auditview";

    public static Pattern VIEW_TYPE_PATTERN = Pattern.compile(VIEW_TYPE_PREVIEW + "|" + VIEW_TYPE_AUDITVIEW + "|" + VIEW_TYPE_FULLVIEW);

    public static final HashMap<String, String> VIEW_TYPE_TO_INFO_FIELD_MAP = new HashMap<String, String>() {{
        put(VIEW_TYPE_PREVIEW, SOLR_PREVIEW_FIELDS_ID_FIELD_NAME);
        put(VIEW_TYPE_AUDITVIEW, SOLR_AUDIT_FIELDS_ID_FIELD_NAME);
        put(VIEW_TYPE_FULLVIEW, VIEW_TYPE_FULLVIEW);
    }};

    public static Pattern COPYSOURCE_PATTERN       = Pattern.compile("^" + SchemaField.class.getName() + ":(.*)\\{.*");
    public static Pattern MATCH_ALL_PATTERN        = Pattern.compile(".*");

    public static String getContentDispositionFileAttachHeader(String fileName) {
        return "attachment; fileName=" + fileName;
    }
}
