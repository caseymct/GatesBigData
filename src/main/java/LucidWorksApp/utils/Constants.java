package LucidWorksApp.utils;

import org.apache.solr.client.solrj.SolrQuery;

public class Constants {
    public static final String PRODUCTION_SERVER    = "http://denlx006.dn.gates.com";
    public static final String PRODUCTION_HOSTNAME  = "denlx006";

    public static final int SOLR_PORT               = 8984;
    public static final int ZOOKEEPER_PORT          = 2181;
    public static final String SOLR_SERVER          = PRODUCTION_SERVER + ":" + SOLR_PORT;
    public static final String ZOOKEEPER_SERVER     = PRODUCTION_SERVER + ":" + ZOOKEEPER_PORT;

    public static final String UTF8                 = "UTF-8";
    public static final String UTF8_CHARSET_ENC     = "charset=" + UTF8;
    public static final String JSON_CONTENT_TYPE    = "application/json";
    public static final String CSV_CONTENT_TYPE     = "text/csv";
    public static final String ZIP_CONTENT_TYPE     = "application/zip";
    public static final String TEXT_CONTENT_TYPE    = "text/plain";
    public static final String FLASH_CONTENT_TYPE   = "application/x-shockwave-flash";
    public static final String IMG_CONTENT_TYPE     = "image/png";

    public static final String SWF_FILE_EXT         = "swf";
    public static final String IMG_FILE_EXT         = "png";
    public static final String TEXT_FILE_EXT        = "txt";
    public static final String JSON_FILE_EXT        = "json";
    public static final String PDF_FILE_EXT         = "pdf";

    public static final String CONTENT_TYPE_HEADER   = "Content-Type";
    public static final String CONTENT_LENGTH_HEADER = "Content-Length";
    public static final String CONTENT_TYPE_VALUE    = JSON_CONTENT_TYPE + "; " + UTF8_CHARSET_ENC;

    public static final String DEFAULT_DELIMETER     = ",";
    public static final String DEFAULT_NEWLINE       = "\n";

    public static int INVALID_INTEGER               = -99999;
    public static double INVALID_DOUBLE             = -99999.99999;

    public static final String SOLR_CONTENT_FIELD_NAME          = "content";
    public static final String SOLR_CONTENT_TYPE_FIELD_NAME     = "content_type";
    public static final String SOLR_ID_FIELD_NAME               = "id";
    public static final String SOLR_SCORE_FIELD_NAME            = "score";

    public static final String SOLR_QUERY_PARAM                 = "q";
    public static final String SOLR_QUERY_DEFAULT               = "*:*";
    public static final String SOLR_FILTERQUERY_PARAM           = "fq";
    public static final String SOLR_FIELDLIST_PARAM             = "fl";
    public static final String SOLR_WT_PARAM                    = "wt";
    public static final String SOLR_WT_DEFAULT                  = "json";
    public static final String SOLR_NUMTERMS_PARAM              = "numTerms";
    public static final String SOLR_SHOWSCHEMA_PARAM            = "show";
    public static final String SOLR_SHOWSCHEMA_DEFAULT          = "schema";
    public static final String SOLR_START_PARAM                 = "start";
    public static final int SOLR_START_DEFAULT                  = 0;
    public static final String SOLR_ROWS_PARAM                  = "rows";
    public static final int SOLR_ROWS_DEFAULT                   = 10;
    public static final String SOLR_INDENT_PARAM                = "indent";
    public static final String SOLR_INDENT_DEFAULT              = "true";
    public static final String SOLR_HIGHLIGHT_PARAM             = "hl";
    public static final String SOLR_HIGHLIGHT_FRAGSIZE_PARAM    = SOLR_HIGHLIGHT_PARAM + ".fragsize";
    public static final String SOLR_HIGHLIGHT_PRE_PARAM         = SOLR_HIGHLIGHT_PARAM + ".simple.pre";
    public static final String SOLR_HIGHLIGHT_PRE_DEFAULT       = "<span class='highlight_text'>";
    public static final String SOLR_HIGHLIGHT_POST_PARAM        = SOLR_HIGHLIGHT_PARAM + ".simple.post";
    public static final String SOLR_HIGHLIGHT_POST_DEFAULT      = "</span>";
    public static final String SOLR_HIGHLIGHT_SNIPPETS_PARAM    = SOLR_HIGHLIGHT_PARAM + ".snippets";
    public static final String SOLR_HIGHLIGHT_SNIPPETS_DEFAULT  = "" + 3;
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
    public static final String SOLR_GROUP_DEFAULT               = "true";
    public static final String SOLR_GROUP_FIELD_PARAM           = "group.field";
    public static final String SOLR_GROUP_SORT_PARAM            = "group.sort";
    public static final String SOLR_GROUP_SORT_DEFAULT          = "score desc";
    public static final SolrQuery.ORDER SOLR_SORT_ORDER_DEFAULT = SolrQuery.ORDER.asc;

    public static final String SOLR_RESPONSE_HIGHLIGHTING_KEY   = "highlighting";
    public static final String SOLR_RESPONSE_SEARCHRESPONSE_KEY = "response";
    public static final String SOLR_RESPONSE_FACET_KEY          = "facet_counts";
    public static final String SOLR_RESPONSE_FACET_FIELDS_KEY   = "facet_fields";
    public static final String SOLR_RESPONSE_FACET_DATES_KEY    = "facet_dates";

    public static final String SOLR_ERRORSTRING_KEY             = "ERROR";
}
