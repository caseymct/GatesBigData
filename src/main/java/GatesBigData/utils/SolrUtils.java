package GatesBigData.utils;

import GatesBigData.constants.solr.*;
import model.schema.CollectionSchemaInfo;
import model.search.FacetFieldEntry;
import model.search.FacetFieldEntryList;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.SimpleOrderedMap;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static GatesBigData.constants.solr.FieldNames.*;
import static GatesBigData.utils.DateUtils.getDateFromDateString;
import static GatesBigData.utils.DateUtils.getSolrDate;
import static GatesBigData.utils.HttpClientUtils.ping;
import static GatesBigData.utils.URLUtils.*;
import static GatesBigData.constants.solr.Solr.*;
import static GatesBigData.constants.solr.QueryParams.*;
import static GatesBigData.utils.Utils.*;
import static GatesBigData.constants.solr.Defaults.*;

public class SolrUtils {

    public static final String FACET_FIELD_REQ_DELIM            = "<field>";
    public static final String FACET_VALUES_REQ_DELIM           = "<values>";
    public static final String FACET_VALUE_OPTS_REQ_DELIM       = "<op>";
    public static final String FACET_DATE_RANGE_REQ_DELIM       = "<to>";

    public static final String SORT_FIELD_REQ_DELIM             = "<sortfield>";
    public static final String SORT_ORDER_REQ_DELIM             = "<sortorder>";

    public static Pattern SORT_PATTERN = Pattern.compile("(.*?)" + SORT_ORDER_REQ_DELIM + "(.*)");

    public static String getRelevantSuggestionField(SolrDocument doc) {
        for(String name : doc.getFieldNames()) {
           if (!ignoreSuggestionFieldName(name)) {
               return name;
           }
        }
        return "";
    }

    private static String fieldQueryPartialString(String field, String word) {
        boolean hasSpecialChars = hasCharsToEncode(word);
        if (hasSpecialChars) {
            word = escape(word);
        }

        return "+" + field + ":" + word + (hasSpecialChars ? "" : "*");
    }

    public static String getSuggestionQuery(String field, String userInput) {
        if (userInput.length() == 1) {
            return fieldQueryPartialString(field, userInput);
        }

        String ret = "";
        for(String piece : encodeQuery(userInput).split("%20| ")) {
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

    public static List<String> getValidFieldNamesSubset(Collection<String> fieldNames) {
        List<String> fieldNamesSubset = new ArrayList<String>();

        for(String fieldName : fieldNames) {
            if (!ignoreFieldName(fieldName, Arrays.asList(ID))) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    public static String getSolrServerURI() {
        for(String url: CLOUD_SOLR_SERVERS) {
            if (ping(url)) return url;
        }
        return null;
    }

    public static String getSolrServerURI(String collection) {
        String uri = getSolrServerURI();
        return uri != null ? constructAddress(uri, collection) : null;
    }

    public static String getSolrServerZkURI() {
        String uri = getSolrServerURI();
        return uri != null ? constructAddress(uri, ZK_ENDPOINT) : null;
    }

    public static String getZkServerURI() {
        return constructAddress(HTTP_PROTOCOL, PRODUCTION_ZK_SERVER, ZOOKEEPER_PORT);
    }

    public static String getSolrZkConfigsDirURI() {
        HashMap<String, String> urlParams = new HashMap<String, String>() {{
            put("path", "/configs");
        }};
        String uri = getSolrServerZkURI();
        return uri != null ? constructAddress(uri, urlParams) : null;
    }

    public static String getSolrLukeURI(String collection) {
        HashMap<String, String> urlParams = new HashMap<String, String>() {{
            put("wt", "json");
        }};
        String uri = getSolrServerURI(collection);
        return uri != null ? constructAddress(uri, Arrays.asList(LUKE_ENDPOINT), urlParams) : null;
    }

    public static String getSolrZkSchemaURI(final String collectionName) {
        HashMap<String, String> urlParams = new HashMap<String, String>() {{
            put("detail", "true");
            put("path", "/configs/" + collectionName + "/" + SOLR_SCHEMA_FILE);
        }};
        String uri = getSolrServerZkURI();
        return uri != null ? constructAddress(uri, urlParams) : null;
    }

    public static String getSolrSchemaURI(String collection) {
        HashMap<String, String> urlParams = new HashMap<String, String>() {{
            put(CONTENT_TYPE_HEADER, XML_CONTENT_TYPE);
            put(CHARSET_ENC_KEY, UTF8);
            put(FILE, SOLR_SCHEMA_FILE);
        }};

        String uri = getSolrServerURI(collection);
        return uri != null ? constructAddress(uri, Arrays.asList(collection, "admin", "file"), urlParams) : null;
    }

    public static String getSolrClusterStateURI() {
        HashMap<String, String> params = new HashMap<String, String>() {{
            put("detail", "true");
            put("path", "/" + ZK_CLUSTERSTATE_FILE);
        }};
        String uri = getSolrServerZkURI();
        return uri != null ? constructAddress(uri, params) : null;
    }

    public static SolrQuery.ORDER getSortOrder(String sortOrder) {
        if (sortOrder == null) {
            return SORT_ORDER;
        }
        return sortOrder.equals(SolrQuery.ORDER.asc.toString()) ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }

    public static String getDocumentContentType(SolrDocument doc) {
        return getFieldStringValue(doc, CONTENT_TYPE, TEXT_CONTENT_TYPE);
    }

    public static Object getFieldValue(SolrDocument doc, String fieldName) {
        return (doc != null && doc.containsKey(fieldName)) ? doc.get(fieldName) : null;
    }

    public static String getFieldStringValue(SolrDocument doc, String fieldName, String defaultValue) {
        Object val = getFieldValue(doc, fieldName);
        if (val == null) {
            return defaultValue;
        }

        return val instanceof ArrayList ? StringUtils.join((ArrayList) val, ",") : val.toString();
    }

    public static Date getFieldDateValue(SolrDocument doc, String fieldName) {
        Object val = getFieldValue(doc, fieldName);
        if (val == null) {
            return null;
        }
        if (val instanceof Date) {
            return (Date) val;
        }
        return getDateFromDateString(val.toString());
    }

    public static String getResponseHeaderParam(QueryResponse rsp, String param) {
        SimpleOrderedMap paramMap = (SimpleOrderedMap) rsp.getHeader().get(Response.HEADER_PARAMS_KEY);
        String paramValue = (String) paramMap.get(param);
        return paramValue != null ? paramValue : "";
    }

    public static boolean isLocalDirectory(SolrDocument doc) {
        if (doc.containsKey(TITLE)) {
            Object title = doc.get(TITLE);
            String titleContents = "";
            if (title instanceof ArrayList) {
                titleContents = (String) ((ArrayList) title).get(0);
            } else if (title instanceof String) {
                titleContents = (String) title;
            }
            return titleContents.startsWith("Index of /");
        }
        return false;
    }

    private static String getFQDate(String dateString) {
        return dateString.equals("*") ? "*" : getSolrDate(dateString);
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
            Set<String> values = entry.getValue();
            if (values.remove(NULL_STRING)) {
                fqStr += "-" + entry.getKey() + ":[* TO *] ";
            }
            if (values.size() > 0) {
                String value = StringUtils.join(values, " OR ");
                if (values.size() > 1) {
                    value = "(" + value + ")";
                }
                fqStr += "+" + entry.getKey() + ":" + value + " ";
            }
        }

        return fqStr.trim();
    }

    public static String composeFilterQuery(String fq, CollectionSchemaInfo info) {
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
                if (info.fieldTypeIsNumber(field)) {
                    values.add(value);
                } else if (info.fieldTypeIsDate(field)) {
                    values.add(getFQDateString(value));
                } else {
                    values.add(quoteStringIfNotNull(value));
                }
            }
            fqComponents = setFQValues(fqComponents, field, values);
        }

        return composeFQString(fqComponents);
    }

    private static String quoteStringIfNotNull(String s) {
        return s.equals(NULL_STRING) ? NULL_STRING : "\"" + s + "\"";
    }

    public static List<SolrQuery.SortClause> getSortClauses(String sortInfo, String sortOrder) {
        if (sortInfo == null) {
            return null;
        }
        if (sortOrder == null || sortInfo.contains(SORT_FIELD_REQ_DELIM)) {
            return getSortClauses(sortInfo);
        }
        List<SolrQuery.SortClause> sortClauses = new ArrayList<SolrQuery.SortClause>();
        sortClauses.add(new SolrQuery.SortClause(sortInfo, sortOrder));
        return sortClauses;
    }

    public static List<SolrQuery.SortClause> getSortClauses(String sortInfo) {
        List<SolrQuery.SortClause> sortClauses = new ArrayList<SolrQuery.SortClause>();

        if (sortInfo == null) {
            sortClauses.add(SORT_CLAUSE);
        } else {
            for(String sortClauseData : sortInfo.split(SORT_FIELD_REQ_DELIM)) {
                if (nullOrEmpty(sortClauseData)) continue;

                Matcher m = SORT_PATTERN.matcher(sortClauseData);
                if (m.matches()) {
                    sortClauses.add(new SolrQuery.SortClause(m.group(1), getSortOrder(m.group(2))));
                }
            }
        }
        return sortClauses;
    }

    public static List<SolrQuery.SortClause> createSortClauseList(String sortField, String sortOrder) {
        return createSortClauseList(sortField, getSortOrder(sortOrder));
    }

    public static List<SolrQuery.SortClause> createSortClauseList(String sortField, SolrQuery.ORDER sortOrder) {
        sortField = returnIfNotNull(sortField, SORT_FIELD_DEFAULT);
        sortOrder = returnIfNotNull(sortOrder, SORT_ORDER);
        return Arrays.asList(new SolrQuery.SortClause(sortField, sortOrder));
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

    public static FacetFieldEntryList constructFacetFieldEntryList(Collection facetFields, FacetFieldEntryList allFacetFields) {
        if (nullOrEmpty(facetFields)) {
            return allFacetFields;
        }

        FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();

        for(Object facetField : facetFields) {
            FacetFieldEntry entry = allFacetFields.get(facetField.toString());
            if (entry != null) {
                facetFieldEntryList.add(entry);
            }
        }

        return facetFieldEntryList;
    }
}
