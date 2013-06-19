package GatesBigData.constants.solr;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public class FieldNames {
    public static final String PREFIX_SUFFIX        = "Prefix";
    public static final String SUGGEST_SUFFIX       = "Suggest";
    public static final String FACET_SUFFIX         = ".facet";
    public static final String THUMBNAIL            = "thumbnail";
    public static final String CORE_TITLE           = "coreTitle";
    public static final String STRUCTURED_DATA      = "structuredData";
    public static final String SUGGESTION_CORE      = "suggestionCore";
    public static final String CONTENT              = "content";
    public static final String TIMESTAMP            = "timestamp";
    public static final String TSTAMP               = "tstamp";
    public static final String HDFSKEY              = "HDFSKey";
    public static final String HDFSSEGMENT          = "HDFSSegment";
    public static final String BOOST                = "boost";
    public static final String HOST                 = "host";
    public static final String DIGEST               = "digest";
    public static final String SEGMENT              = "segment";
    public static final String VERSION              = "_version_";
    public static final String CONTENT_TYPE         = "content_type";
    public static final String ID                   = "id";
    public static final String CORE                 = "core";
    public static final String COLLECTION           = "collection";
    public static final String LANG                 = "lang";
    public static final String URL                  = "url";
    public static final String SCORE                = "score";
    public static final String COUNT                = "count";
    public static final String TITLE                = "title";
    public static final String LAST_MODIFIED        = "last_modified";
    public static final String CREATE_DATE          = "creation_date";
    public static final String LAST_AUTHOR          = "last_author";
    public static final String APPLICATION_NAME     = "application_name";
    public static final String AUTHOR               = "author";
    public static final String COMPANY              = "company";
    public static final String CACHE                = "cache";
    public static final String SIGNATURE_FIELD      = "signatureField";

    public static final String FACET_FIELDS         = "FACETFIELDS";
    public static final String PREVIEW_FIELDS       = "PREVIEWFIELDS";
    public static final String AUDIT_FIELDS         = "AUDITFIELDS";
    public static final String TABLE_FIELDS         = "TABLEFIELDS";
    public static final String WORDTREE_FIELDS      = "WORDTREEFIELDS";
    public static final String X_AXIS_FIELDS        = "XAXIS";
    public static final String Y_AXIS_FIELDS        = "YAXIS";
    public static final String SERIES_FIELDS        = "SERIES";
    public static final String DISPLAY_NAMES_FIELDS = "DISPLAY_NAMES";
    public static final String REPORT_TITLES_FIELDS = "REPORT_TITLES";
    public static final String REPORT_DATA_FIELDS   = "REPORT_DATA";

    public static final HashMap<String, String> METADATA_TO_SOLRFIELDS = new HashMap<String, String>() {{
        put("Last-Save-Date",   LAST_MODIFIED);
        put("Creation-Date",    CREATE_DATE);
        put("Last-Author",      LAST_AUTHOR);
        put("Application-Name", APPLICATION_NAME);
        put("Author",           AUTHOR);
        put("Company",          COMPANY);
        put("title",            TITLE);
    }};

    public static List<String> INFO_FIELD_NAMES = Arrays.asList(WORDTREE_FIELDS, AUDIT_FIELDS, PREVIEW_FIELDS, TABLE_FIELDS,
                                                                FACET_FIELDS, X_AXIS_FIELDS, Y_AXIS_FIELDS, SERIES_FIELDS,
                                                                DISPLAY_NAMES_FIELDS, REPORT_TITLES_FIELDS, REPORT_DATA_FIELDS);
    public static Pattern INFO_FIELDS_PATTERN   = Pattern.compile(StringUtils.join(INFO_FIELD_NAMES, "|"));


    public static final String ATTR_PREFIX      = "attr";
    public static List<String> FIELDNAMES_TOIGNORE_LIST = Arrays.asList(VERSION, BOOST, HDFSKEY, DIGEST, HOST, SEGMENT, TSTAMP);
    public static Pattern FIELDNAMES_TOIGNORE           = Pattern.compile("^(" + ATTR_PREFIX + "|_).*|.*(" +
                                                                        StringUtils.join(FIELDNAMES_TOIGNORE_LIST, "|") + ").*");

    public static List<String> VIEW_FIELDNAMES_TOIGNORE_LIST = Arrays.asList(VERSION, CONTENT, CORE_TITLE, HDFSKEY, HDFSSEGMENT,
                                                                            STRUCTURED_DATA, TITLE, CONTENT_TYPE, ID, TIMESTAMP,
                                                                            URL, CACHE, SUGGESTION_CORE, TSTAMP, SIGNATURE_FIELD,
                                                                            LANG, BOOST, DIGEST, HOST, SEGMENT, TSTAMP, PREFIX_SUFFIX,
                                                                            FACET_SUFFIX, SUGGEST_SUFFIX);
    public static Pattern VIEW_FIELD_NAMES_TOIGNORE          = Pattern.compile("^(" + ATTR_PREFIX + "|_).*|.*(" +
                                                                        StringUtils.join(VIEW_FIELDNAMES_TOIGNORE_LIST, "|") + ").*");

    public static List<String> SUGGESTION_FIELDNAMES_TOIGNORE_LIST = Arrays.asList(ID, CONTENT, COUNT, TIMESTAMP, VERSION);
    public static Pattern SUGGESTION_FIELDNAMES_TOIGNORE           = Pattern.compile(StringUtils.join(SUGGESTION_FIELDNAMES_TOIGNORE_LIST, "|"));

    public static String getFacetDisplayName(String name) {
        return name.endsWith(FACET_SUFFIX) ? name.substring(0, name.lastIndexOf(FACET_SUFFIX)) : name;
    }

    public static boolean isInfoField(String f) {
        return INFO_FIELDS_PATTERN.matcher(f).matches();
    }

    public static boolean ignoreFieldName(String fieldName) {
        return VIEW_FIELD_NAMES_TOIGNORE.matcher(fieldName).matches();
    }

    public static boolean validFieldName(String fieldName) {
        return !FIELDNAMES_TOIGNORE.matcher(fieldName).matches();
    }

    public static boolean ignoreSuggestionFieldName(String f) {
        return SUGGESTION_FIELDNAMES_TOIGNORE.matcher(f).matches();
    }
}
