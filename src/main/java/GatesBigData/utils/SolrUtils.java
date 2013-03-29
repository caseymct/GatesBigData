package GatesBigData.utils;

import model.FacetFieldEntry;
import model.FacetFieldEntryList;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
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
    public static final String UPDATECSV_ENDPOINT    = "update/csv";
    public static final String ADMIN_FILE_ENDPOINT   = "admin/file";

    public static final String SOLR_SCHEMA_HDFSKEY              = "HDFSKey";
    public static final String SOLR_SCHEMA_HDFSSEGMENT          = "HDFSSegment";
    public static final String SOLR_SCHEMA_PREFIXFIELD_ENDSWITH = "Prefix";

    public static final String[] SOLR_CHARS_TO_ENCODE   = {  "-", "\\+", "\\[", "\\]", "\\(", "\\)",   ":"};
    public static final String[] SOLR_CHARS_ENCODED_VAL = {"%96", "%2B", "%5B", "%5D", "%28", "%29", "%3A"};

    static Pattern FIELDNAMES_TOIGNORE = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
                                                         "HDFSKey|boost|digest|host|segment|tstamp).*");


    static Pattern VIEW_FIELD_NAMES_TOIGNORE = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|content|coreTitle|" +
            "HDFSKey|HDFSSegment|structuredData|title|content_type|id|timestamp|url|cache|" +
            "tstamp|signatureField|lang|boost|digest|host|segment|tstamp|Prefix|Suggest|facet).*");

    static Pattern SUGGESTION_FIELDNAMES_TOIGNORE = Pattern.compile(Constants.SOLR_ID_FIELD_NAME + "|" +
            Constants.SOLR_CONTENT_FIELD_NAME   + "|" + Constants.SOLR_COUNT_FIELD_NAME + "|" +
            Constants.SOLR_TIMESTAMP_FIELD_NAME + "|" + Constants.SOLR_VERSION_FIELD_NAME);

    static Pattern SOLR_INFOFIELDS_PATTERN = Pattern.compile(StringUtils.join(Constants.SOLR_ALL_INFO_FIELDS, "|"));

    public static String removeFacetSuffix(String fieldName) {
        if (fieldName.endsWith(Constants.SOLR_FACET_FIELD_SUFFIX)) {
            return fieldName.substring(0, fieldName.indexOf(Constants.SOLR_FACET_FIELD_SUFFIX));
        }
        return fieldName;
    }

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
        Matcher m = VIEW_FIELD_NAMES_TOIGNORE.matcher(fieldName);
        return m.matches();
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

    public static double getDoubleValueIfExists(Map<String, Double> map, String key) {
        return map.containsKey(key) ? map.get(key) : 0.0;
    }

    public static boolean validFieldName(String fieldName, boolean isFacetField) {
        Matcher m = FIELDNAMES_TOIGNORE.matcher(fieldName);
        return (!isFacetField && fieldName.equals(SOLR_SCHEMA_HDFSKEY)) || !m.matches();
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
        urlParams.put(Constants.SOLR_FILE_PARAM, Constants.SOLR_SCHEMA_DEFAULT_FILE_NAME);

        return new Path(getSolrServerURI(coreName), ADMIN_FILE_ENDPOINT).toString() + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrSelectURI(String coreName, HashMap<String,String> urlParams) {
        return getSolrSelectURI(coreName) + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrSelectURI(String coreName, HashMap<String,String> urlParams, HashMap<String, List<String>> repeatKeyUrlParams) {
        return getSolrSelectURI(coreName) + Utils.constructUrlParams(urlParams, repeatKeyUrlParams);
    }

    public static String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams) {
        return getSolrServerURI(coreName) + "/" + UPDATECSV_ENDPOINT + Utils.constructUrlParams(urlParams);
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
        String contentType = (String) getFieldValue(doc, Constants.SOLR_CONTENT_TYPE_FIELD_NAME);
        return !Utils.nullOrEmpty(contentType) ? contentType : Constants.TEXT_CONTENT_TYPE;
    }

    public static List<String> getFieldsFromInfoString(String infoString) {
        List<String> fieldList = new ArrayList<String>();
        for(String fullField : infoString.split(",")) {
            fieldList.add(fullField.split(":")[0].trim());
        }
        return fieldList;
    }

    public static boolean nameIsInfoField(String name) {
        Matcher m = SOLR_INFOFIELDS_PATTERN.matcher(name.toUpperCase());
        return m.matches();
    }

    public static Object getFieldValue(SolrDocument doc, String fieldName) {
        return (doc != null && doc.containsKey(fieldName)) ? doc.get(fieldName) : null;
    }

    public static String getFieldStringValue(SolrDocument doc, String fieldName, String defaultValue) {
        if (doc == null || !doc.containsKey(fieldName)) {
            return defaultValue;
        }
        Object val = doc.get(fieldName);
        if (val instanceof ArrayList) {
            return StringUtils.join((ArrayList) val, ",");
        }
        return val.toString();
    }

    public static SolrQuery setResponseFormatAsJSON(SolrQuery query) {
        query.add(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);
        return query;
    }

    public static String getResponseHeaderParam(QueryResponse rsp, String param) {
        SimpleOrderedMap paramMap = (SimpleOrderedMap) rsp.getHeader().get(Constants.SOLR_RESPONSE_HEADER_PARAMS_KEY);
        String paramValue = (String) paramMap.get(param);
        return paramValue != null ? paramValue : "";
    }

    public static String getSuggestionCoreName(String coreName) {
        return coreName + Constants.SOLR_SUGGEST_CORE_SUFFIX;
    }

    public static boolean isLocalDirectory(SolrDocument doc) {
        if (doc.containsKey(Constants.SOLR_TITLE_FIELD_NAME)) {
            Object title = doc.get(Constants.SOLR_TITLE_FIELD_NAME);
            String titleContents = "";
            if (title instanceof ArrayList) {
                titleContents = (String) ((ArrayList) doc.get(Constants.SOLR_TITLE_FIELD_NAME)).get(0);
            } else if (title instanceof String) {
                titleContents = (String) title;
            }
            return titleContents.startsWith("Index of /");
        }
        return false;
    }

    public static String editFilterQueryDateRange(String fq, String fieldName) {
        if (fq == null || !fq.contains(fieldName)) return fq;
        Pattern p = Pattern.compile(".*\\+" + fieldName + ":([\\(|\\[])([^\\)|\\]]*)([\\)|\\]])(\\+.*)*");
        Matcher m = p.matcher(fq);
        if (m.matches()) {
            String newDateFq = "";
            String[] newDates = m.group(2).split("\" \"");

            for(String d : newDates) {
                String[] dates = d.replaceAll("^\"|\"$", "").split("\\s(-|TO|to)\\s");
                newDateFq += fieldName + ":[" + DateUtils.getSolrDate(dates[0]) + " TO " + DateUtils.getSolrDate(dates[1]) + "] ";
            }
            // If it's just one date, then use the + to indicate AND
            if (newDates.length == 1) {
                newDateFq = "+" + newDateFq;
            }
            fq = fq.replace("+" + fieldName + ":" + m.group(1) + m.group(2) + m.group(3), newDateFq);
        }
        return fq;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for (SolrDocument doc : docs) {
            String hdfsSeg = (String) doc.getFieldValue(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add((String) doc.getFieldValue(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }

    public static FacetFieldEntryList constructFacetFieldEntryList(String facetFieldInfo, SolrCollectionSchemaInfo schemaInfo) {
        FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();

        if (Utils.nullOrEmpty(facetFieldInfo)) {
            return schemaInfo.getFacetFieldEntryList();
        } else {
            FacetFieldEntryList allFacetFields = schemaInfo.getFacetFieldEntryList();

            for(String ret : facetFieldInfo.split(",")) {
                String[] n = ret.split(":");
                FacetFieldEntry entry = allFacetFields.get(n[0]);

                if (entry != null) {
                    facetFieldEntryList.add(entry);
                }
            }
        }

        return facetFieldEntryList;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(JSONArray docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for(int i = 0; i < docs.size(); i++) {
            JSONObject doc = docs.getJSONObject(i);
            String hdfsSeg = doc.getString(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add(doc.getString(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }
}
