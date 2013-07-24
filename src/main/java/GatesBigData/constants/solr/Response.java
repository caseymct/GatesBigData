package GatesBigData.constants.solr;

import org.apache.solr.client.solrj.response.UpdateResponse;

public class Response {
    public static final String HIGHLIGHTING_KEY           = "highlighting";
    public static final String RESPONSE_KEY               = "response";
    public static final String HEADER_PARAMS_KEY          = "params";
    public static final String FACET_KEY                  = "facet_counts";
    public static final String NAME_KEY                   = "name";
    public static final String FIELDNAMES_KEY             = "fieldnames";
    public static final String VALUES_KEY                 = "values";
    public static final String NUM_FOUND_KEY              = "num_found";
    public static final String DOCS_KEY                   = "docs";
    public static final String SUGGESTION_KEY             = "suggestions";
    public static final String COLLECTION_KEY             = "collection";
    public static final String COLLECTIONS_KEY            = "collections";
    public static final String TITLE_KEY                  = "title";
    public static final String STRUCTURED_KEY             = "structured";
    public static final String DATE_RANGE_START_KEY       = "start";
    public static final String DATE_RANGE_END_KEY         = "end";

    public static final int CODE_SUCCESS = 0;
    public static final int CODE_ERROR   = -1;

    public static boolean success(UpdateResponse rsp) {
        return rsp.getStatus() == CODE_SUCCESS;
    }

    public static boolean success(int code) {
        return code == CODE_SUCCESS;
    }
}
