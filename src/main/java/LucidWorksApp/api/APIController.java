package LucidWorksApp.api;


public abstract class APIController {
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String CONTENT_TYPE_VALUE = "application/json; charset=UTF-8";
    public static final String CONTENT_LENGTH_HEADER = "Content-Length";

    public static final String PARAM_CORE_NAME = "core";
    public static final String PARAM_FIELD_NAME = "field";
    public static final String PARAM_FILE_NAME = "file";
    public static final String PARAM_FILE_ONSERVER = "local";
    public static final String PARAM_HDFS = "hdfs";
    public static final String PARAM_VIEWTYPE = "view";
    public static final String PARAM_USER_INPUT = "userinput";
    public static final String PARAM_PREFIX_FIELD = "prefixfield";
    public static final String PARAM_HDFSSEGMENT = "segment";

    public static final String PARAM_QUERY = "query";
    public static final String PARAM_START = "start";
    public static final String PARAM_ROWS = "rows";
    public static final String PARAM_SORT_TYPE = "sort";
    public static final String PARAM_SORT_ORDER = "order";
    public static final String PARAM_FQ = "fq";

    public static final String PARAM_NUM_SUGGESTIONS_PER_FIELD = "n";
}
