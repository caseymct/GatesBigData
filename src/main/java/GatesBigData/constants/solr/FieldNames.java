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
    public static final String LAST_SAVE_DATE       = "last_save_date";
    public static final String CREATION_DATE        = "creation_date";
    public static final String LAST_AUTHOR          = "last_author";
    public static final String APPLICATION_NAME     = "application_name";
    public static final String AUTHOR               = "author";
    public static final String COMPANY              = "company";
    public static final String CACHE                = "cache";
    public static final String SIGNATURE_FIELD      = "signatureField";

    public static final List<String> METADATA_SOLRFIELDS = Arrays.asList(LAST_MODIFIED, CREATION_DATE, LAST_AUTHOR, APPLICATION_NAME,
                                                                         AUTHOR, COMPANY, TITLE);
    public static String getSolrFieldFromMetaData(String metadataField) {
        metadataField = metadataField.toLowerCase().replaceAll("-", "_");
        if (metadataField.equals(LAST_SAVE_DATE)) {
            return LAST_MODIFIED;
        }
        int i = METADATA_SOLRFIELDS.indexOf(metadataField);
        return i > -1 ? METADATA_SOLRFIELDS.get(i) : null;
    }

    public static Pattern FIELDNAMES_TOIGNORE = Pattern.compile("^(_).*|.*(" +
            StringUtils.join(Arrays.asList(
                    VERSION, CONTENT, CORE_TITLE, HDFSKEY, HDFSSEGMENT, STRUCTURED_DATA, TITLE, CONTENT_TYPE, ID, TIMESTAMP,
                    URL, CACHE, SUGGESTION_CORE, TSTAMP, SIGNATURE_FIELD, LANG, BOOST, DIGEST, HOST, SEGMENT, TSTAMP, PREFIX_SUFFIX,
                    FACET_SUFFIX, SUGGEST_SUFFIX), "|") + ").*");

    public static Pattern SUGGESTION_FIELDNAMES_TOIGNORE = Pattern.compile(
            StringUtils.join(Arrays.asList(ID, CONTENT, COUNT, TIMESTAMP, VERSION), "|"));

    public static String getFacetDisplayName(String name) {
        return name.endsWith(FACET_SUFFIX) ? name.substring(0, name.lastIndexOf(FACET_SUFFIX)) : name;
    }

    public static boolean isFieldName(String actualFieldName, String solrFieldName) {
        return actualFieldName.toLowerCase().trim().equals(solrFieldName);
    }

    public static boolean ignoreFieldName(String fieldName) {
        return FIELDNAMES_TOIGNORE.matcher(fieldName).matches();
    }

    public static boolean ignoreFieldName(String fieldName, List<String> idsToKeep) {
        if (ignoreFieldName(fieldName)) {
            for(String idToKeep : idsToKeep) {
                if (isFieldName(fieldName, idToKeep)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean ignoreSuggestionFieldName(String f) {
        return SUGGESTION_FIELDNAMES_TOIGNORE.matcher(f).matches();
    }
}
