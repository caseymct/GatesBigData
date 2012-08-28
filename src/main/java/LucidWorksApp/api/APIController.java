package LucidWorksApp.api;


public abstract class APIController {
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String CONTENT_TYPE_VALUE = "application/json; charset=UTF-8";

    public static final String PARAM_CORE_NAME = "core";
    public static final String PARAM_FILENAME = "file";
    public static final String PARAM_FILE_ONSERVER = "local";
    public static final String PARAM_HDFS = "hdfs";
}
