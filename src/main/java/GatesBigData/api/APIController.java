package GatesBigData.api;


import model.SolrCollectionSchemaInfo;
import org.apache.solr.client.solrj.SolrServer;

import javax.servlet.http.HttpSession;

public abstract class APIController {
    public static final String PARAM_CORE_NAME      = "core";
    public static final String PARAM_FIELD_NAME     = "field";
    public static final String PARAM_FILE_NAME      = "file";
    public static final String PARAM_FILE_ONSERVER  = "local";
    public static final String PARAM_HDFS           = "hdfs";
    public static final String PARAM_VIEWTYPE       = "view";
    public static final String PARAM_USER_INPUT     = "userinput";
    public static final String PARAM_PREFIX_FIELD   = "prefixfield";
    public static final String PARAM_HDFSSEGMENT    = "segment";
    public static final String PARAM_ID             = "id";
    public static final String PARAM_HIGHLIGHTING   = "hl";

    public static final String PARAM_QUERY          = "query";
    public static final String PARAM_START          = "start";
    public static final String PARAM_ROWS           = "rows";
    public static final String PARAM_SORT_FIELD     = "sort";
    public static final String PARAM_SORT_ORDER     = "order";
    public static final String PARAM_FQ             = "fq";

    public static final String PARAM_N_THREADS      = "nthreads";
    public static final String PARAM_N_FILES        = "nfiles";

    public static final String PARAM_NUM_SUGGESTIONS_PER_FIELD = "n";

    public static final String SESSION_HDFSDIR_TOKEN     = "currentHDFSDir";
    public static final String SESSION_SCHEMA_INFO_TOKEN = "schemaInfo";

    public SolrCollectionSchemaInfo getSolrCollectionSchemaInfo(SolrServer server, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_SCHEMA_INFO_TOKEN + coreName;

        SolrCollectionSchemaInfo schemaInfo = (SolrCollectionSchemaInfo) session.getAttribute(sessionKey);
        if (schemaInfo == null) {
            schemaInfo = new SolrCollectionSchemaInfo(coreName, server);
            session.setAttribute(sessionKey, schemaInfo);
        }

        return schemaInfo;
    }
}
