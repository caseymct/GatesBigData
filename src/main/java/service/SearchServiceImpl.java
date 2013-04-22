package service;

import GatesBigData.utils.*;
import model.*;
import net.sf.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.Base64;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.Date;


public class SearchServiceImpl implements SearchService {

    private static final Logger logger = Logger.getLogger(SearchServiceImpl.class);
    private CoreService coreService;

    @Autowired
    public void setServices(CoreService coreService) {
        this.coreService = coreService;
    }

    public List<String> getFieldsToWrite(SolrDocument originalDoc, String coreName, String viewType) {

        if (viewType.equals(Constants.VIEW_TYPE_AUDITVIEW) || viewType.equals(Constants.VIEW_TYPE_PREVIEW)) {
            String fieldsStr = getCoreInfoFieldValue(coreName, Constants.VIEW_TYPE_TO_INFO_FIELD_MAP.get(viewType));
            if (!Utils.nullOrEmpty(fieldsStr)) {
                List<String> fields = Arrays.asList(fieldsStr.split(","));
                Collections.sort(fields);
                return fields;
            }
        }

        return SolrUtils.getAllViewFields(originalDoc);
    }

    public SolrDocumentList getResultList(String coreName, String queryStr, String fq, String sortField, SolrQuery.ORDER sortOrder,
                                          int start, int rows, String viewFields) {
        return getResultList(buildQuery(coreName, queryStr, fq, sortField, sortOrder, start, rows, null, viewFields), coreName);
    }

    public SolrDocumentList getResultList(ExtendedSolrQuery query, String coreName) {
        return execQuery(query, coreName).getResults();
    }

    public SolrDocument getRecord(ExtendedSolrQuery query, String coreName) {
        SolrDocumentList docs = getResultList(query, coreName);
        return docs.getNumFound() >= 1 ? docs.get(0) : null;
    }

    public SolrDocument getRecord(String coreName, String id) {
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put(Constants.SOLR_FIELD_NAME_ID, id);

        return getRecord(coreName, queryParams, Constants.SOLR_BOOLEAN_DEFAULT);
    }

    public SolrDocument getRecord(String coreName, HashMap<String, String> queryParams, String op) {
        return getRecord(new ExtendedSolrQuery(queryParams, op), coreName);
    }

    public String getRecordCoreTitle(String coreName) {
        SolrDocument doc = getRecord(buildTitleResultQuery(coreName), coreName);
        return SolrUtils.getFieldStringValue(doc, Constants.SOLR_FIELD_NAME_CORE_TITLE, null);
    }

    public SolrDocument getRecordByFieldValue(String field, String value, String coreName) {
        return getRecord(buildSingleResultQuery(coreName, field + ":" + value), coreName);
    }

    public String getCoreInfoFieldValue(String coreName, String fieldName) {
        fieldName = fieldName.toUpperCase();
        if (!SolrUtils.nameIsInfoField(fieldName)) {
            return "";
        }

        SolrDocument doc = getRecordByFieldValue(Constants.SOLR_FIELD_NAME_TITLE, fieldName, coreName);
        return SolrUtils.getFieldStringValue(doc, fieldName, null).trim();
    }

    public List<String> getCoreInfoFieldValues(String coreName, String fieldName) {
        String fieldString = getCoreInfoFieldValue(coreName, fieldName);
        return Arrays.asList(StringUtils.split(fieldString, ","));
    }

    private String getThumbnailRecord(String coreName, String id) {
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put(Constants.SOLR_FIELD_NAME_ID, id);
        queryParams.put(Constants.SOLR_FIELD_NAME_CORE, coreName);

        SolrDocument doc = getRecord(Constants.SOLR_THUMBNAILS_CORE_NAME, queryParams, Constants.SOLR_BOOLEAN_UNION);
        Object thumbnail = SolrUtils.getFieldValue(doc, Constants.SOLR_FIELD_NAME_THUMBNAIL);

        if (!Utils.nullOrEmpty(thumbnail)) {
            return Constants.BASE64_CONTENT_TYPE + "," + Base64.encodeBase64String((byte []) thumbnail);
        }
        return null;
    }

    public void printRecord(String coreName, String id, String viewType, boolean isStructuredData, StringWriter writer) {
        if (Utils.nullOrEmpty(id)) return;

        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);
            g.writeStartObject();

            if (isStructuredData) {
                printRecord(coreName, id, viewType, g);
            } else {
                String thumbnailRecord = getThumbnailRecord(coreName, id);
                if (thumbnailRecord != null) {
                    printThumbnailRecord(thumbnailRecord, id, g);
                }
            }

            g.writeEndObject();
            g.close();
        } catch (JsonGenerationException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    private void printRecord(String coreName, String id, String viewType, JsonGenerator g) throws IOException {
        SolrDocument doc = getRecord(coreName, id);
        g.writeObjectFieldStart(Constants.SOLR_FIELD_NAME_CONTENT);

        String currParent = "";
        boolean writingSubObj = false;

        for(String field : getFieldsToWrite(doc, coreName, viewType)) {
            String val;
            if (!doc.containsKey(field)) {
                val = "<i>Field does not exist in this database</i>";

            } else {
                val = (String) doc.get(field);

                if (field.contains(".")) {
                    String[] t = field.split("\\.");
                    String newParent = t[0];

                    if (!newParent.equals(currParent)) {
                        if (writingSubObj) {
                            g.writeEndObject();
                        }
                        g.writeObjectFieldStart(newParent);
                        currParent = newParent;
                        writingSubObj = true;
                    }
                    field = t[1];
                } else if (writingSubObj) {
                    g.writeEndObject();
                    writingSubObj = false;
                }
            }

            Utils.writeValueByType(field, val, g);
        }

        g.writeEndObject();
    }

    private void printThumbnailRecord(String thumbnailRecord, String id, JsonGenerator g) throws IOException {
        Utils.writeValueByType(Constants.SOLR_FIELD_NAME_URL, id, g);
        Utils.writeValueByType(Constants.SOLR_FIELD_NAME_CONTENT_TYPE, Constants.IMG_CONTENT_TYPE, g);
        g.writeStringField(Constants.SOLR_FIELD_NAME_CONTENT, thumbnailRecord);
    }

    private SuggestionList getTopNListings(GroupResponse response, String fullField, int n) {
        SuggestionList suggestionList = new SuggestionList();
        List<Group> groups = response.getValues().get(0).getValues();

        for(int i = 0; i < groups.size() && suggestionList.size() < n; i++) {
            SolrDocumentList docList = groups.get(i).getResult();
            if (docList.size() == 0) continue;

            SolrDocument doc = docList.get(0);
            long numFound = docList.getNumFound();
            float score = (Float) doc.get(Constants.SOLR_FIELD_NAME_SCORE);
            String text = Utils.getUTF8String((String) doc.get(fullField));
            suggestionList.add(text, fullField, numFound, score);
        }

        return suggestionList;
    }

    public JSONArray suggestUsingGrouping(String coreName, String userInput, Map<String, String> fieldMap, int listingsPerField) {
        SuggestionList suggestions = new SuggestionList();

        for(Map.Entry<String, String> entry : fieldMap.entrySet()) {
            String prefixField = entry.getKey();
            String fullField   = entry.getValue();
            String queryStr    = SolrUtils.getSuggestionQuery(prefixField, userInput);
            String viewFields  = Constants.SOLR_FIELD_NAME_SCORE + "," + fullField;

            GroupResponse rsp = getGroupResponse(coreName, queryStr, null, Constants.SOLR_DEFAULT_VALUE_ROWS, viewFields,
                    fullField, Constants.SOLR_SUGGESTION_GROUP_LIMIT);
            if (rsp == null) continue;

            suggestions.concat(getTopNListings(rsp, fullField, listingsPerField));
        }

        return suggestions.getSortedFormattedSuggestionJSONArray();
    }

    public JSONArray suggestUsingSeparateCore(String coreName, String userInput, int n) {
        SuggestionList suggestions = new SuggestionList();

        String queryString = SolrUtils.getSuggestionQuery(Constants.SOLR_FIELD_NAME_CONTENT, userInput);
        SolrDocumentList docs = getResultList(buildSuggestQuery(coreName, queryString, n), coreName);

        int listMaxSize = Math.min(docs.size(), n);

        for(int i = 0; i < listMaxSize && suggestions.size() < listMaxSize; i++) {
            SolrDocument doc = docs.get(i);
            String field = SolrUtils.getRelevantSuggestionField(doc);
            String fieldValue = SolrUtils.getFieldStringValue(doc, field, "");
            long count = (Long) doc.getFieldValue(Constants.SOLR_FIELD_NAME_COUNT);

            if (!fieldValue.equals("") && count > 0) {
                suggestions.add(fieldValue, SolrUtils.getFacetDisplayName(field), count, 1.0);
            }
        }

        return suggestions.getFormattedSuggestionJSONArray();
    }

    public void solrSearch(String queryString, String coreName, String sortField, String sortOrder, int start, int rows,
                           String fq, FacetFieldEntryList facetFields, String viewFields, List<String> dateFields,
                           StringWriter writer) throws IOException {

        ExtendedSolrQuery query = buildHighlightQuery(coreName, queryString, fq, sortField, SolrUtils.getSortOrder(sortOrder),
                                                      start, rows, facetFields, viewFields);
        QueryResponse rsp = execQuery(query, coreName);
        writeResponse(true, rsp, dateFields, query.toString(), writer);
    }

    public List<String> getFieldCounts(String coreName, String queryString, String fq, List<String> facetFields, boolean separateFacetCount)
            throws IOException {

        ExtendedSolrQuery query = buildCountsPerFieldQuery(coreName, queryString, fq, facetFields, separateFacetCount);
        QueryResponse rsp = execQuery(query, coreName);
        long numFound = rsp.getResults().getNumFound();

        Map<String, Number> fieldsAndCounts = new HashMap<String, Number>();

        for(FacetField field : rsp.getFacetFields()) {
            List<FacetField.Count> values = field.getValues();
            long n;
            if (separateFacetCount) {
                n = values.size();
            } else {
                long numNull = numFound;
                if (values.size() > 0) {
                    FacetField.Count value = field.getValues().get(0);
                    if (value.getName() == null) {
                        numNull = value.getCount();
                    }
                }
                n = numFound - numNull;
            }

            fieldsAndCounts.put(field.getName(), n);
        }

        fieldsAndCounts = Utils.sortByValue(fieldsAndCounts);
        List<String> fields = new ArrayList<String>();
        for(Map.Entry entry : fieldsAndCounts.entrySet()) {
            fields.add(entry.getKey() + " (" + entry.getValue() + ")");
        }
        return fields;
    }

    public boolean recordExists(String coreName, HashMap<String, String> fqList) {
        return recordExists(coreName, buildRecordExistsQuery(fqList));
    }

    public boolean recordExists(String coreName, String id) {
        return recordExists(coreName, buildRecordExistsQuery(id));
    }

    public boolean recordExists(String coreName, ExtendedSolrQuery query) {
        QueryResponse rsp = execQuery(query, coreName);
        return rsp != null && rsp.getResults().getNumFound() > 0;
    }

    public SeriesPlot getPlotData(String coreName, String queryString, String fq, int numFound, String xAxisField,
                                  boolean xAxisIsDate, String yAxisField, boolean yAxisIsDate, String seriesField,
                                  boolean seriesIsDate) {
        ExtendedSolrQuery query = buildPlotQuery(coreName, queryString, fq, numFound, xAxisField, yAxisField, seriesField);
        SolrDocumentList docs = getResultList(query, coreName);

        return new SeriesPlot(xAxisField, xAxisIsDate, yAxisField, yAxisIsDate, seriesField, seriesIsDate, docs);
    }

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer) throws IOException {
        ExtendedSolrQuery query = buildFacetOnlyQuery(coreName, facetFields);
        QueryResponse rsp = execQuery(query, coreName);

        writeResponse(false, rsp, null, query.toString(), writer);
    }

    public GroupResponse getGroupResponse(String coreName, String queryStr, String fq, int rows, String viewFields,
                                          String groupField, int groupLimit) {
        ExtendedSolrQuery query = buildGroupQuery(coreName, queryStr, fq, rows, viewFields, groupField, groupLimit);
        QueryResponse rsp = execQuery(query, coreName);
        return rsp != null ? rsp.getGroupResponse() : null;
    }

    private ExtendedSolrQuery buildDateQuery(String coreName, String field, boolean fieldIsDate, SolrQuery.ORDER order) {
        String fq = fieldIsDate ? field + ":[NOW-100YEARS TO NOW]" : null;  //If this is a date field, make sure we get reasonable responses
        return buildQuery(coreName, Constants.SOLR_DEFAULT_VALUE_QUERY, fq, field, order, 0, 1, null, field);
    }

    private ExtendedSolrQuery buildPlotQuery(String coreName, String queryString, String fq, int rows, String xAxisField, String yAxisField,
                                             String seriesField) {
        String viewFields = xAxisField + "," + yAxisField;
        if (!Utils.nullOrEmpty(seriesField)) {
            viewFields += "," + seriesField;
        }

        ExtendedSolrQuery query = buildQuery(coreName, queryString, fq, xAxisField, SolrQuery.ORDER.asc, 0, rows, null, viewFields);
        query.addSortField(yAxisField, SolrQuery.ORDER.asc);
        return query;
    }

    private ExtendedSolrQuery buildCountsPerFieldQuery(String coreName, String queryStr, String fq, List<String> facetFields,
                                                       boolean separateFacetCounts) {
        ExtendedSolrQuery query = buildQuery(coreName, queryStr, fq, null, null, 0, 0, null, null);
        for(String facetField : facetFields) {
            query.addFacetField(facetField);
        }

        query.setFacet(true);
        query.setFacetMissing(!separateFacetCounts);
        query.setFacetLimit(separateFacetCounts ? -1 : 1);
        query.setFacetMinCount(separateFacetCounts ? 1 : (int) coreService.getCoreInfo(coreName).getJSONObject("index").getLong("numDocs"));

        return query;
    }

    private ExtendedSolrQuery buildTitleResultQuery(String coreName) {
        return buildQuery(coreName, Constants.SOLR_DEFAULT_VALUE_QUERY, null, Constants.SOLR_DEFAULT_VALUE_SORT_FIELD,
                          Constants.SOLR_DEFAULT_VALUE_SORT_ORDER, Constants.SOLR_DEFAULT_VALUE_START, 1, null,
                          Constants.SOLR_FIELD_NAME_CORE_TITLE);
    }

    private ExtendedSolrQuery buildSingleResultQuery(String coreName, String queryString) {
        return buildQuery(coreName, queryString, null, Constants.SOLR_DEFAULT_VALUE_SORT_FIELD,
                Constants.SOLR_DEFAULT_VALUE_SORT_ORDER, Constants.SOLR_DEFAULT_VALUE_START, 1, null, null);
    }

    private ExtendedSolrQuery buildFacetOnlyQuery(String coreName, FacetFieldEntryList facetFields) {
        return buildQuery(coreName, Constants.SOLR_DEFAULT_VALUE_QUERY, null, Constants.SOLR_DEFAULT_VALUE_SORT_FIELD,
                          Constants.SOLR_DEFAULT_VALUE_SORT_ORDER, Constants.SOLR_DEFAULT_VALUE_START, 0, facetFields, null);
    }

    private ExtendedSolrQuery buildSuggestQuery(String coreName, String queryString, int rows) {
        ExtendedSolrQuery query = buildQuery(coreName, queryString, null, Constants.SOLR_FIELD_NAME_SCORE,
                SolrQuery.ORDER.desc, 0, rows, null, null);

        query.addSortField(Constants.SOLR_FIELD_NAME_COUNT, SolrQuery.ORDER.desc);
        return query;
    }

    private ExtendedSolrQuery buildRecordExistsQuery(HashMap<String, String> fqList) {
        ExtendedSolrQuery query = new ExtendedSolrQuery(Constants.SOLR_DEFAULT_VALUE_QUERY);
        query.setRows(0);
        for(Map.Entry<String, String> fqEntry : fqList.entrySet()) {
            boolean nullValue = fqEntry.getValue() == null;
            String fieldName  = (nullValue ? "-" : "+") + fqEntry.getKey();
            String fieldValue = nullValue ? "[* TO *]" : "\"" + fqEntry.getValue() + "\"";
            query.addFilterQuery(fieldName + ":" + fieldValue);
        }

        return query;
    }

    private ExtendedSolrQuery buildRecordExistsQuery(String id) {
        ExtendedSolrQuery query = new ExtendedSolrQuery(Constants.SOLR_FIELD_NAME_ID + ":" + id);
        query.setRows(0);
        return query;
    }

    private ExtendedSolrQuery buildGroupQuery(String coreName, String queryString, String fq, int rows, String viewFields,
                                              String groupField, int groupLimit) {
        ExtendedSolrQuery query = buildQuery(coreName, queryString, fq, null, null, 0, rows, null, viewFields);
        query.setGroupingDefaults(groupField);
        query.setGroupLimit(groupLimit);

        return query;
    }

    public ExtendedSolrQuery buildHighlightQuery(String coreName, String queryString, String fq, String sortField,
                                         SolrQuery.ORDER sortOrder, int start, int rows, FacetFieldEntryList facetFields,
                                         String viewFields) {

        ExtendedSolrQuery query = buildQuery(coreName, queryString, fq, sortField,  sortOrder, start, rows, facetFields, viewFields);
        query.setHighlightDefaults();
        return query;
    }

    public ExtendedSolrQuery buildQuery(String coreName, String queryString, String fq, String sortField,
                                         SolrQuery.ORDER sortOrder, int start, int rows, FacetFieldEntryList facetFields,
                                         String viewFields) {

        if (Utils.nullOrEmpty(sortField)) sortField = Constants.SOLR_DEFAULT_VALUE_SORT_FIELD;
        if (Utils.nullOrEmpty(sortOrder)) sortOrder = Constants.SOLR_DEFAULT_VALUE_SORT_ORDER;

        ExtendedSolrQuery query = new ExtendedSolrQuery(queryString);
        query.setStart(start);
        query.setRows(rows);
        query.setViewFields(viewFields);
        query.addSortField(sortField, sortOrder);

        if (!Utils.nullOrEmpty(facetFields)) {
            query.setFacetDefaults();

            for(FacetFieldEntry field : facetFields) {
                String fieldName = field.getFieldName();

                if (field.fieldTypeIsDate() && !field.isMultiValued()) {
                    List<Date> dateRange = getSolrFieldDateRange(coreName, fieldName, true);
                    query.setDateRangeFacet(fieldName, dateRange.get(0), dateRange.get(1), 10);
                } else {
                    query.addFacetField(fieldName);
                }
            }
        }

        query.setFilterQuery(fq);

        return query;
    }

    private Date getSolrDate(String coreName, String field, boolean fieldIsDate, SolrQuery.ORDER order) {
        ExtendedSolrQuery query = buildDateQuery(coreName, field, fieldIsDate, order);
        SolrDocument doc = getRecord(query, coreName);

        Object val = doc.getFieldValue(field);
        if (val instanceof Date) {
            return (Date) val;
        }
        return DateUtils.getDateFromDateString(val.toString());
    }

    public List<Date> getSolrFieldDateRange(String coreName, String field, boolean fieldIsDate) {
        return Arrays.asList(getSolrDate(coreName, field, fieldIsDate, SolrQuery.ORDER.asc),
                             getSolrDate(coreName, field, fieldIsDate, SolrQuery.ORDER.desc));
    }

    public QueryResponse execQuery(ExtendedSolrQuery query, String coreName) {
        QueryResponse rsp = null;
        SolrServer server = coreService.getSolrServer(coreName);

        try {
            long currTimeMillis = System.currentTimeMillis();
            rsp = server.query(query);
            System.out.println("Search time: " + (System.currentTimeMillis() - currTimeMillis));
            System.out.flush();
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }

        return rsp;
    }

    private void writeResponse(boolean includeSearchResults, QueryResponse rsp, List<String> dateFields, String queryString,
                               StringWriter writer) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeObjectFieldStart(Constants.SOLR_RESPONSE_KEY);
        g.writeStringField(Constants.SOLR_PARAM_QUERY, queryString);

        if (includeSearchResults) {
            writeSearchResponse(rsp, dateFields, g);
            writeHighlightResponse(rsp, g);
        }
        writeFacetResponse(rsp, g);

        g.writeEndObject();
        g.close();
    }

    private void writeHighlightResponse(QueryResponse rsp, JsonGenerator g) throws IOException {
        Map<String, Map<String, List<String>>> highlighting = rsp.getHighlighting();
        NamedList header = rsp.getHeader();

        g.writeObjectFieldStart(Constants.SOLR_RESPONSE_HIGHLIGHTING_KEY);

        NamedList params = (NamedList) header.get(Constants.SOLR_RESPONSE_HEADER_PARAMS_KEY);
        g.writeStringField(Constants.SOLR_PARAM_HIGHLIGHT_PRE, (String) params.get(Constants.SOLR_PARAM_HIGHLIGHT_PRE));
        g.writeStringField(Constants.SOLR_PARAM_HIGHLIGHT_POST, (String) params.get(Constants.SOLR_PARAM_HIGHLIGHT_POST));

        for(Object keyObject : highlighting.keySet()) {
            String key = keyObject.toString();
            g.writeObjectFieldStart(key);

            for(Map.Entry<String, List<String>> entry : highlighting.get(key).entrySet()) {
                g.writeArrayFieldStart(entry.getKey());
                for(String val : entry.getValue()) {
                    Utils.writeValueByType(val, g);
                }
                g.writeEndArray();
            }
            g.writeEndObject();
        }
        g.writeEndObject();
    }

    private void writeSearchResponse(QueryResponse rsp, List<String> dateFields, JsonGenerator g) throws IOException {
        SolrDocumentList docs = rsp.getResults();

        g.writeStringField(Constants.SOLR_PARAM_FIELDLIST, (String) ((SimpleOrderedMap) rsp.getHeader().get(Constants.SOLR_RESPONSE_HEADER_PARAMS_KEY)).get(Constants.SOLR_PARAM_FIELDLIST));
        g.writeNumberField(Constants.SEARCH_RESPONSE_NUM_FOUND_KEY, docs.getNumFound());
        g.writeArrayFieldStart(Constants.SEARCH_RESPONSE_NUM_DOCS_KEY);

        for (SolrDocument doc : docs) {
            if (!SolrUtils.isLocalDirectory(doc)) {
                g.writeStartObject();
                for(String fieldName : SolrUtils.getValidFieldNamesSubset(doc.getFieldNames(), false)) {
                    Object val = doc.getFieldValue(fieldName);
                    if (dateFields.contains(fieldName) && val instanceof Date) {
                        val = DateUtils.getFormattedDateString((Date) val, DateUtils.DAY_DATE_FORMAT);
                    }
                    Utils.writeValueByType(fieldName, val, g);
                }
                g.writeEndObject();
            }
        }
        g.writeEndArray();
    }

    private void writeFacets(List<FacetField> facetFieldList, boolean isDate, JsonGenerator g) throws IOException {
        if (Utils.nullOrEmpty(facetFieldList)) return;

        for(FacetField facetField : facetFieldList) {
            List<FacetField.Count> facetFieldValues = facetField.getValues();
            if (facetFieldValues == null) continue;

            g.writeStartObject();
            g.writeStringField(Constants.SOLR_RESPONSE_NAME_KEY, SolrUtils.getFacetDisplayName(facetField.getName()));

            g.writeArrayFieldStart(Constants.SOLR_RESPONSE_VALUES_KEY);
            for(FacetField.Count count : facetFieldValues) {
                String facetFieldDisplayStr = SolrUtils.getFacetFieldDisplayString(count, isDate);
                if (Utils.nullOrEmpty(facetFieldDisplayStr)) continue;

                g.writeString(facetFieldDisplayStr);
            }
            g.writeEndArray();
            g.writeEndObject();
        }
    }

    private void writeFacetResponse(QueryResponse rsp, JsonGenerator g) throws IOException {

        g.writeArrayFieldStart(Constants.SOLR_RESPONSE_FACET_KEY);
        writeFacets(rsp.getFacetFields(), false, g);
        writeFacets(rsp.getFacetDates(), true, g);
        g.writeEndArray();
    }
}
