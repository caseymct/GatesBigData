package GatesBigData.constants;

public class Constants {

    public static boolean USE_PRODUCTION_SOLR_SERVER 	= true;
    public static boolean USE_CLOUD_SOLR_SERVER 	    = false;

    // Invalid numbers
    public static int    INVALID_INTEGER               	= -99999;
    public static long   INVALID_LONG                 	= -99999;
    public static double INVALID_DOUBLE             	= -99999.999999;

    public static final String DEFAULT_NEWLINE      	= System.getProperty("line.separator");
    public static final String DEFAULT_DELIMETER    	= ",";

    public static final String UTF8                 	= "UTF-8";
    public static final String CHARSET_ENC_KEY      	= "charset";
    public static final String JSON_CONTENT_TYPE    	= "application/json";
    public static final String CSV_CONTENT_TYPE     	= "text/csv";
    public static final String ZIP_CONTENT_TYPE     	= "application/zip";
    public static final String TEXT_CONTENT_TYPE    	= "text/plain";
    public static final String XML_CONTENT_TYPE     	= "text/xml";
    public static final String FLASH_CONTENT_TYPE   	= "application/x-shockwave-flash";
    public static final String IMG_CONTENT_TYPE     	= "image/png";
    public static final String BASE64_CONTENT_TYPE  	= "data:image/png;base64";

    public static final String SWF_FILE_EXT         	= "swf";
    public static final String IMG_FILE_EXT         	= "png";
    public static final String TEXT_FILE_EXT        	= "txt";
    public static final String JSON_FILE_EXT        	= "json";
    public static final String PDF_FILE_EXT         	= "pdf";
    public static final String CSV_FILE_EXT         	= "csv";
    public static final String ZIP_FILE_EXT             = "zip";

    public static final String CONTENT_TYPE_HEADER      = "Content-Type";
    public static final String CONTENT_LENGTH_HEADER    = "Content-Length";
    public static final String CONTENT_DISP_HEADER      = "Content-Disposition";
    public static final String CONTENT_TYPE_VALUE       = JSON_CONTENT_TYPE + "; " + CHARSET_ENC_KEY + "=" + UTF8;

    public static final String HTTP_PROTOCOL            = "http://";
    public static final String JDBC_PROTOCOL            = "jdbc:";
    public static final String SMB_PROTOCOL             = "smb://";
    public static final String HDFS_PROTOCOL            = "hdfs://";

    public static final String SMB_DOMAIN               = "NA";
    public static final String SMB_USERNAME             = "bigdatasvc";
    public static final String SMB_PASSWORD             = "Crawl2013";

    public static final String LOCALHOST_SERVER         = "localhost";

    public static String getContentDispositionFileAttachHeader(String fileName) {
        return "attachment; fileName=" + fileName;
    }
}
