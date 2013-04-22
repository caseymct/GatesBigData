package GatesBigData.utils;

import model.FacetFieldEntry;
import model.FacetFieldEntryList;
import model.SolrCollectionSchemaInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.SimpleOrderedMap;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolrUtils {

    public static final String SOLR_ENDPOINT         = "solr";
    public static final String SOLR_SELECT_ENDPOINT  = "select";
    public static final String ADMIN_FILE_ENDPOINT   = "admin/file";

    public static final String FACET_FIELD_REQ_DELIM            = "<field>";
    public static final String FACET_VALUES_REQ_DELIM           = "<values>";
    public static final String FACET_VALUE_OPTS_REQ_DELIM       = "<op>";
    public static final String FACET_DATE_RANGE_REQ_DELIM       = "<to>";

    public static final String[] SOLR_CHARS_TO_ENCODE   = {  "-", "\\+", "\\[", "\\]", "\\(", "\\)",   ":"};
    public static final String[] SOLR_CHARS_ENCODED_VAL = {"%96", "%2B", "%5B", "%5D", "%28", "%29", "%3A"};

    static Pattern FIELDNAMES_TOIGNORE = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
                                                         "HDFSKey|boost|digest|host|segment|tstamp).*");


    static Pattern VIEW_FIELD_NAMES_TOIGNORE = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|content|coreTitle|" +
            "HDFSKey|HDFSSegment|structuredData|title|content_type|id|timestamp|url|cache|" +
            "tstamp|signatureField|lang|boost|digest|host|segment|tstamp|Prefix|Suggest|facet).*");

    static Pattern SUGGESTION_FIELDNAMES_TOIGNORE = Pattern.compile(Constants.SOLR_FIELD_NAME_ID + "|" +
            Constants.SOLR_FIELD_NAME_CONTENT + "|" + Constants.SOLR_FIELD_NAME_COUNT + "|" +
            Constants.SOLR_FIELD_NAME_TIMESTAMP + "|" + Constants.SOLR_FIELD_NAME_VERSION);

    static Pattern SOLR_INFOFIELDS_PATTERN = Pattern.compile(StringUtils.join(Constants.SOLR_INFO_FIELD_NAMES, "|"));

    public static String getRelevantSuggestionField(SolrDocument doc) {
        String field = (String) doc.getFieldNames().toArray()[2];
        Matcher m = SUGGESTION_FIELDNAMES_TOIGNORE.matcher(field);
        if (!m.matches()) return field;

        for(String name : doc.getFieldNames()) {
           m = SUGGESTION_FIELDNAMES_TOIGNORE.matcher(name);
           if (!m.matches()) {
               return field;
           }
        }
        return "";
    }

    public static boolean ignoreFieldName(String fieldName) {
        return VIEW_FIELD_NAMES_TOIGNORE.matcher(fieldName).matches();
    }

    public static boolean validFieldName(String fieldName, boolean isFacetField) {
        Matcher m = FIELDNAMES_TOIGNORE.matcher(fieldName);
        return (!isFacetField && fieldName.equals(Constants.SOLR_FIELD_NAME_HDFSKEY)) || !m.matches();
    }

    public static boolean nameIsInfoField(String name) {
        return SOLR_INFOFIELDS_PATTERN.matcher(name.toUpperCase()).matches();
    }

    public static List<String> getAllViewFields(SolrDocument doc) {
        List<String> viewFields = new ArrayList<String>();

        for(String field : doc.getFieldNames()) {
            if (!ignoreFieldName(field)) {
                viewFields.add(field);
            }
        }

        Collections.sort(viewFields);
        return viewFields;
    }

    private static String fieldQueryPartialString(String field, String word) {
        boolean hasSpecialChars = word.matches(".*(" + StringUtils.join(SOLR_CHARS_TO_ENCODE, "|") + ").*");
        if (hasSpecialChars) {
            word = escapeSolrChars(word);
        }

        return "+" + field + ":" + word + (hasSpecialChars ? "" : "*");
    }

    public static String escapeSolrChars(String word) {
        for(int i = 0; i < SOLR_CHARS_TO_ENCODE.length; i++) {
            word = word.replaceAll(SOLR_CHARS_TO_ENCODE[i], SOLR_CHARS_ENCODED_VAL[i]);
        }
        return word;
    }

    public static String getSuggestionQuery(String field, String userInput) {
        if (userInput.length() == 1) {
            return fieldQueryPartialString(field, userInput);
        }

        String ret = "";
        for(String piece : Utils.encodeQuery(userInput).split("%20| ")) {
            ret += fieldQueryPartialString(field, piece) + " ";
        }

        return ret.trim();
    }

    public static boolean schemaStringIndicatesMultiValued(String schema) {
        return schema.length() > 4 && schema.charAt(3) == 77;
    }

    public static String stripHighlightHtml(String s, String hlPre, String hlPost) {
        return s.replaceAll(hlPre + "|" + hlPost, "");
    }

    public static List<String> getValidFieldNamesSubset(Collection<String> fieldNames, boolean isFacetField) {
        List<String> fieldNamesSubset = new ArrayList<String>();

        for(String fieldName : fieldNames) {
            if (validFieldName(fieldName, isFacetField)) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    public static String getSolrServerURI(String coreName) {
        return Utils.addToUrlIfNotEmpty(Constants.SOLR_SERVER + "/" + SOLR_ENDPOINT, coreName);
    }

    public static String getSolrSelectURI(String coreName) {
        return new Path(getSolrServerURI(coreName), SOLR_SELECT_ENDPOINT).toString();
    }

    public static String getSolrSchemaURI(String coreName) {
        HashMap<String, String> urlParams = new HashMap<String, String>();
        urlParams.put(Constants.CONTENT_TYPE_HEADER, Constants.XML_CONTENT_TYPE);
        urlParams.put(Constants.CHARSET_ENC_KEY, Constants.UTF8);
        urlParams.put(Constants.SOLR_PARAM_FILE, Constants.SOLR_SCHEMA_DEFAULT_FILE_NAME);

        return new Path(getSolrServerURI(coreName), ADMIN_FILE_ENDPOINT).toString() + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrCreateCollectionURI(String coreName, String name, int numShards, int replicationFactor) {
        HashMap<String, String> urlParams = new HashMap<String, String>();
        urlParams.put("action", "CREATE");
        urlParams.put("name", name);
        urlParams.put("numShards", numShards + "");
        urlParams.put("replicationFactor", replicationFactor + "");

        return getSolrServerURI(coreName) + "/admin/collections" + Utils.constructUrlParams(urlParams);
    }

    public static SolrQuery.ORDER getSortOrder(String sortOrder) {
        return sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }

    public static String getDocumentContentType(SolrDocument doc) {
        String contentType = (String) getFieldValue(doc, Constants.SOLR_FIELD_NAME_CONTENT_TYPE);
        return !Utils.nullOrEmpty(contentType) ? contentType : Constants.TEXT_CONTENT_TYPE;
    }

    public static List<String> getFieldsFromInfoString(String infoString) {
        List<String> fieldList = new ArrayList<String>();
        for(String fullField : infoString.split(",")) {
            fieldList.add(fullField.split(":")[0].trim());
        }
        return fieldList;
    }

    public static Object getFieldValue(SolrDocument doc, String fieldName) {
        return (doc != null && doc.containsKey(fieldName)) ? doc.get(fieldName) : null;
    }

    public static String getFieldStringValue(SolrDocument doc, String fieldName, String defaultValue) {
        if (doc == null || !doc.containsKey(fieldName)) {
            return defaultValue;
        }
        Object val = doc.get(fieldName);
        return (val instanceof ArrayList) ? StringUtils.join((ArrayList) val, ",") : val.toString();
    }

    public static String getResponseHeaderParam(QueryResponse rsp, String param) {
        SimpleOrderedMap paramMap = (SimpleOrderedMap) rsp.getHeader().get(Constants.SOLR_RESPONSE_HEADER_PARAMS_KEY);
        String paramValue = (String) paramMap.get(param);
        return paramValue != null ? paramValue : "";
    }

    public static String getSuggestionCoreName(String coreName) {
        return coreName + Constants.SOLR_SUGGEST_CORE_SUFFIX;
    }

    public static String getFacetDisplayName(String name) {
        if (name.endsWith(Constants.SOLR_FIELD_TYPE_FACET_SUFFIX)) {
            return name.substring(0, name.lastIndexOf(Constants.SOLR_FIELD_TYPE_FACET_SUFFIX));
        }
        return name;
    }

    public static boolean isLocalDirectory(SolrDocument doc) {
        if (doc.containsKey(Constants.SOLR_FIELD_NAME_TITLE)) {
            Object title = doc.get(Constants.SOLR_FIELD_NAME_TITLE);
            String titleContents = "";
            if (title instanceof ArrayList) {
                titleContents = (String) ((ArrayList) doc.get(Constants.SOLR_FIELD_NAME_TITLE)).get(0);
            } else if (title instanceof String) {
                titleContents = (String) title;
            }
            return titleContents.startsWith("Index of /");
        }
        return false;
    }

    private static String getFQDate(String dateString) {
        return dateString.equals("*") ? "*" : DateUtils.getSolrDate(dateString);
    }

    private static String getFQDateString(String dateString) {
        List<String> dates = Arrays.asList(dateString.split(FACET_DATE_RANGE_REQ_DELIM));
        return "[" + getFQDate(dates.get(0)) + " TO " + getFQDate(dates.get(1)) + "]";
    }

    private static HashMap<String, Set<String>> setFQValues(HashMap<String, Set<String>> fqComponents, String field, HashSet<String> newValues) {
        Set<String> v = fqComponents.containsKey(field) ? fqComponents.get(field) : new HashSet<String>();
        v.addAll(newValues);
        fqComponents.put(field, v);
        return fqComponents;
    }

    private static String composeFQString(HashMap<String, Set<String>> fqComponents) {
        String fqStr = "";
        for(Map.Entry<String, Set<String>> entry : fqComponents.entrySet()) {
            String value = StringUtils.join(entry.getValue(), " OR ");
            if (entry.getValue().size() > 1) {
                value = "(" + value + ")";
            }
            fqStr += "+" + entry.getKey() + ":" + value + " ";
        }

        return fqStr.trim();
    }

    public static String composeFilterQuery(String fq, SolrCollectionSchemaInfo schemaInfo) {
        if (Utils.nullOrEmpty(fq)) {
            return null;
        }

        HashMap<String, Set<String>> fqComponents = new HashMap<String, Set<String>>();

        for(String fqComponent : fq.split(FACET_FIELD_REQ_DELIM)) {
            if (Utils.nullOrEmpty(fqComponent)) continue;

            String[] fqComponentArray = fqComponent.split(FACET_VALUES_REQ_DELIM);
            String field              = fqComponentArray[0];
            HashSet<String> values    = new HashSet<String>();

            for(String value : fqComponentArray[1].split(FACET_VALUE_OPTS_REQ_DELIM)) {
                if (schemaInfo.fieldTypeIsNumber(field)) {
                    values.add(value);
                } else if (schemaInfo.fieldTypeIsDate(field)) {
                    values.add(getFQDateString(value));
                } else {
                    values.add("\"" + value + "\"");
                }
            }
            fqComponents = setFQValues(fqComponents, field, values);
        }

        return composeFQString(fqComponents);
    }

    public static String getFacetFieldDisplayString(FacetField.Count count, boolean isDate) {
        String fieldName = count.getName();
        String facetFieldDisplayStr = fieldName;

        if (isDate) {
            Date startDate = DateUtils.getDateFromDateString(fieldName);
            Date endDate   = DateUtils.addGapToDate(startDate, count.getFacetField().getGap());

            if (startDate == null || endDate == null) {
                return null;
            }

            String startDateStr  = DateUtils.getFormattedDateString(startDate, DateUtils.FULL_MONTH_DATE_FORMAT);
            String endDateStr    = DateUtils.getFormattedDateString(endDate, DateUtils.FULL_MONTH_DATE_FORMAT);
            facetFieldDisplayStr = startDateStr + " to " + endDateStr;
        }
        return facetFieldDisplayStr + " (" + count.getCount() + ")";
    }

    public static FacetFieldEntryList constructFacetFieldEntryList(String facetFieldInfo, SolrCollectionSchemaInfo schemaInfo) {
        FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();

        if (Utils.nullOrEmpty(facetFieldInfo)) {
            return schemaInfo.getFacetFieldEntryList();
        }

        FacetFieldEntryList allFacetFields = schemaInfo.getFacetFieldEntryList();

        for(String ret : facetFieldInfo.split(",")) {
            String[] n = ret.split(":");
            FacetFieldEntry entry = allFacetFields.get(n[0]);
            if (entry != null) {
                facetFieldEntryList.add(entry);
            }
        }

        return facetFieldEntryList;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for (SolrDocument doc : docs) {
            String hdfsSeg = (String) doc.getFieldValue(Constants.SOLR_FIELD_NAME_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add((String) doc.getFieldValue(Constants.SOLR_FIELD_NAME_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }
}
