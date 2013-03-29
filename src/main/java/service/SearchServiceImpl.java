package service;

import GatesBigData.utils.*;
import model.*;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
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
import org.apache.solr.schema.DateField;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


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
        queryParams.put(Constants.SOLR_ID_FIELD_NAME, id);

        return getRecord(coreName, queryParams, Constants.SOLR_DEFAULT_BOOLEAN_OP);
    }

    public SolrDocument getRecord(String coreName, HashMap<String, String> queryParams, String op) {
        return getRecord(new ExtendedSolrQuery(queryParams, op), coreName);
    }

    public String getRecordCoreTitle(String coreName) {
        SolrDocument doc = getRecord(buildTitleResultQuery(coreName), coreName);
        return SolrUtils.getFieldStringValue(doc, Constants.SOLR_CORE_TITLE_FIELD_NAME, null);
    }

    public SolrDocument getRecordByFieldValue(String field, String value, String coreName) {
        return getRecord(buildSingleResultQuery(coreName, field + ":" + value), coreName);
    }

    public String getCoreInfoFieldValue(String coreName, String fieldName) {
        fieldName = fieldName.toUpperCase();
        if (!SolrUtils.nameIsInfoField(fieldName)) {
            return "";
        }

        SolrDocument doc = getRecordByFieldValue(Constants.SOLR_TITLE_FIELD_NAME, fieldName, coreName);
        return SolrUtils.getFieldStringValue(doc, fieldName, null);
    }

    private String getThumbnailRecord(String coreName, String id) {
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put(Constants.SOLR_ID_FIELD_NAME, id);
        queryParams.put(Constants.SOLR_CORE_FIELD_NAME, coreName);

        SolrDocument doc = getRecord(Constants.SOLR_THUMBNAILS_CORE_NAME, queryParams, Constants.SOLR_UNION_BOOLEAN_OP);
        Object thumbnail = SolrUtils.getFieldValue(doc, Constants.SOLR_THUMBNAIL_FIELD_NAME);

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
        g.writeObjectFieldStart(Constants.SOLR_CONTENT_FIELD_NAME);

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
        Utils.writeValueByType(Constants.SOLR_URL_FIELD_NAME, id, g);
        Utils.writeValueByType(Constants.SOLR_CONTENT_TYPE_FIELD_NAME, Constants.IMG_CONTENT_TYPE, g);
        g.writeStringField(Constants.SOLR_CONTENT_FIELD_NAME, thumbnailRecord);
    }

    private SuggestionList getTopNListings(GroupResponse response, String fullField, int n) {
        SuggestionList suggestionList = new SuggestionList();
        List<Group> groups = response.getValues().get(0).getValues();

        for(int i = 0; i < groups.size() && suggestionList.size() < n; i++) {
            SolrDocumentList docList = groups.get(i).getResult();
            if (docList.size() == 0) continue;

            SolrDocument doc = docList.get(0);
            long numFound = docList.getNumFound();
            float score = (Float) doc.get(Constants.SOLR_SCORE_FIELD_NAME);
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
            String viewFields  = Constants.SOLR_SCORE_FIELD_NAME + "," + fullField;

            GroupResponse rsp = getGroupResponse(coreName, queryStr, null, Constants.SOLR_ROWS_DEFAULT, viewFields,
                    fullField, Constants.SOLR_SUGGESTION_GROUP_LIMIT);
            if (rsp == null) continue;

            suggestions.concat(getTopNListings(rsp, fullField, listingsPerField));
        }

        return suggestions.getSortedFormattedSuggestionJSONArray();
    }

    public JSONArray suggestUsingSeparateCore(String coreName, String userInput, int n) {
        SuggestionList suggestions = new SuggestionList();

        String queryString = SolrUtils.getSuggestionQuery(Constants.SOLR_CONTENT_FIELD_NAME, userInput);
        SolrDocumentList docs = getResultList(buildSuggestQuery(coreName, queryString, n), coreName);

        int listMaxSize = Math.min(docs.size(), n);

        for(int i = 0; i < listMaxSize && suggestions.size() < listMaxSize; i++) {
            SolrDocument doc = docs.get(i);
            String field = SolrUtils.getRelevantSuggestionField(doc);
            String fieldValue = SolrUtils.getFieldStringValue(doc, field, "");
            long count = (Long) doc.getFieldValue(Constants.SOLR_COUNT_FIELD_NAME);

            if (!fieldValue.equals("") && count > 0) {
                suggestions.add(fieldValue, SolrUtils.removeFacetSuffix(field), count, 1.0);
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
        writeResponse(true, rsp, dateFields, writer);
    }

    public List<String> getFieldCounts(String coreName, String queryString, String fq, List<String> facetFields, boolean separateFacetCount)
            throws IOException {

        ExtendedSolrQuery query = buildCountsPerFieldQuery(coreName, queryString, fq, facetFields, separateFacetCount);
        QueryResponse rsp = execQuery(query, coreName);
        long numFound = rsp.getResults().getNumFound();

        List<String> fieldsAndCounts = new ArrayList<String>();
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

            fieldsAndCounts.add(field.getName() + " (" + n + ")");
        }

        return fieldsAndCounts;
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

        writeResponse(false, rsp, null, writer);
    }

    public GroupResponse getGroupResponse(String coreName, String queryStr, String fq, int rows, String viewFields,
                                       String groupField, int groupLimit) {
        ExtendedSolrQuery query = buildGroupQuery(coreName, queryStr, fq, rows, viewFields, groupField, groupLimit);
        QueryResponse rsp = execQuery(query, coreName);
        return rsp != null ? rsp.getGroupResponse() : null;
    }

    private ExtendedSolrQuery buildDateQuery(String coreName, String field, SolrQuery.ORDER order) {
        return buildQuery(coreName, field + ":*", null, field, order, Constants.SOLR_START_DEFAULT, 1, null, field);
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
        return buildQuery(coreName, Constants.SOLR_QUERY_DEFAULT, null, Constants.SOLR_SORT_FIELD_DEFAULT,
                          Constants.SOLR_SORT_ORDER_DEFAULT, Constants.SOLR_START_DEFAULT, 1, null,
                          Constants.SOLR_CORE_TITLE_FIELD_NAME);
    }

    private ExtendedSolrQuery buildSingleResultQuery(String coreName, String queryString) {
        return buildQuery(coreName, queryString, null, Constants.SOLR_SORT_FIELD_DEFAULT,
                Constants.SOLR_SORT_ORDER_DEFAULT, Constants.SOLR_START_DEFAULT, 1, null, null);
    }

    private ExtendedSolrQuery buildFacetOnlyQuery(String coreName, FacetFieldEntryList facetFields) {
        return buildQuery(coreName, Constants.SOLR_QUERY_DEFAULT, null, Constants.SOLR_SORT_FIELD_DEFAULT,
                          Constants.SOLR_SORT_ORDER_DEFAULT, Constants.SOLR_START_DEFAULT, 0, facetFields, null);
    }

    private ExtendedSolrQuery buildSuggestQuery(String coreName, String queryString, int rows) {
        ExtendedSolrQuery query = buildQuery(coreName, queryString, null, Constants.SOLR_SCORE_FIELD_NAME,
                SolrQuery.ORDER.desc, 0, rows, null, null);

        query.addSortField(Constants.SOLR_COUNT_FIELD_NAME, SolrQuery.ORDER.desc);
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

        ExtendedSolrQuery query = buildQuery(coreName, queryString, fq, sortField,  sortOrder, start, rows, facetFields,
                                            viewFields);
        query.setHighlightDefaults();
        query.setHighlightFields(viewFields);
        query.setHighlightQuery(queryString, fq);

        return query;
    }

    public ExtendedSolrQuery buildQuery(String coreName, String queryString, String fq, String sortField,
                                         SolrQuery.ORDER sortOrder, int start, int rows, FacetFieldEntryList facetFields,
                                         String viewFields) {

        if (Utils.nullOrEmpty(sortField)) sortField = Constants.SOLR_SORT_FIELD_DEFAULT;
        if (Utils.nullOrEmpty(sortOrder)) sortOrder = Constants.SOLR_SORT_ORDER_DEFAULT;

        ExtendedSolrQuery query = new ExtendedSolrQuery(queryString);
        query.setStart(start);
        query.setRows(rows);
        query.setViewFields(viewFields);
        query.addSortField(sortField, sortOrder);

        if (!Utils.nullOrEmpty(facetFields)) {
            query.setFacetDefaults(facetFields.size());

            for(FacetFieldEntry field : facetFields) {
                String fieldName = field.getFieldName();

                if (field.fieldTypeIsDate() && !field.isMultiValued()) {
                    List<String> dateRange = getSolrFieldDateRange(coreName, fieldName, DateUtils.SOLR_DATE);
                    query.addDateRangeFacet(fieldName, DateUtils.getDateFromSolrDate(dateRange.get(0)),
                            DateUtils.getDateFromSolrDate(dateRange.get(1)), dateRange.get(2));
                    fq = SolrUtils.editFilterQueryDateRange(fq, fieldName);
                } else {
                    query.addFacetField(fieldName);
                }
            }
        }

        query.setFilterQuery(fq);

        return query;
    }

    private String getSolrDate(String coreName, String field, String format, SolrQuery.ORDER order) {
        ExtendedSolrQuery query = buildDateQuery(coreName, field, order);
        SolrDocument doc = getRecord(query, coreName);

        Object val = doc.getFieldValue(field);
        if (val instanceof Date) {
            String solrDate = DateUtils.getSolrDate(val.toString());
            return DateUtils.getFormattedDateStringFromSolrDate(solrDate, format);
        }
        return "";
    }

    public List<String> getSolrFieldDateRange(String coreName, String field, String format) {
        List<String> dateRange = new ArrayList<String>();
        int buckets = 10;

        dateRange.add(getSolrDate(coreName, field, format, SolrQuery.ORDER.asc));
        dateRange.add(getSolrDate(coreName, field, format, SolrQuery.ORDER.desc));

        if (format.equals(DateUtils.SOLR_DATE)) {
            dateRange.add(DateUtils.getDateGapString(dateRange.get(0), dateRange.get(1), buckets));
        }
        return dateRange;
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

    private void writeResponse(boolean includeSearchResults, QueryResponse rsp, List<String> dateFields, StringWriter writer) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeObjectFieldStart(Constants.SOLR_RESPONSE_KEY);

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
        g.writeStringField(Constants.SOLR_HIGHLIGHT_PRE_PARAM, (String) params.get(Constants.SOLR_HIGHLIGHT_PRE_PARAM));
        g.writeStringField(Constants.SOLR_HIGHLIGHT_POST_PARAM, (String) params.get(Constants.SOLR_HIGHLIGHT_POST_PARAM));

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

        g.writeStringField(Constants.SOLR_FIELDLIST_PARAM, (String) ((SimpleOrderedMap) rsp.getHeader().get(Constants.SOLR_RESPONSE_HEADER_PARAMS_KEY)).get(Constants.SOLR_FIELDLIST_PARAM));
        g.writeNumberField(Constants.SEARCH_RESPONSE_NUM_FOUND_KEY, docs.getNumFound());
        g.writeArrayFieldStart(Constants.SEARCH_RESPONSE_NUM_DOCS_KEY);

        for (SolrDocument doc : docs) {
            if (!SolrUtils.isLocalDirectory(doc)) {
                g.writeStartObject();
                for(String fieldName : SolrUtils.getValidFieldNamesSubset(doc.getFieldNames(), false)) {
                    Object val = doc.getFieldValue(fieldName);
                    if (dateFields.contains(fieldName) && val instanceof Date) {
                        val = new SimpleDateFormat("dd-MMM-yy").format(val);
                    }
                    Utils.writeValueByType(fieldName, val, g);
                    /*String[] subFields = StringUtils.split(fieldName, ".");
                    if (subFields.length > 1) {
                        g.writeObjectFieldStart(subFields[0]);
                        Utils.writeValueByType(subFields[1], doc.getFieldValue(fieldName), g);
                        g.writeEndObject();
                    } else {
                        Utils.writeValueByType(fieldName, doc.getFieldValue(fieldName), g);
                    } */
                }
                g.writeEndObject();
            }
        }
        g.writeEndArray();
    }

    private void writeFacetResponse(QueryResponse rsp, JsonGenerator g) throws IOException {

        g.writeArrayFieldStart(Constants.SOLR_RESPONSE_FACET_KEY);
        for(FacetField facetField : rsp.getFacetFields()) {
            if (facetField.getValues() == null) continue;

            g.writeStartObject();
            String name = facetField.getName();
            if (name.endsWith(Constants.SOLR_FACET_FIELD_SUFFIX)) {
                name = name.substring(0, name.lastIndexOf(Constants.SOLR_FACET_FIELD_SUFFIX));
            }
            g.writeStringField("name", name);

            g.writeArrayFieldStart("values");
            for(FacetField.Count count : facetField.getValues()) {
                if (count.getCount() > 0) {
                    g.writeString(count.getName() + " (" + count.getCount() + ")");
                }
            }
            g.writeEndArray();
            g.writeEndObject();
        }

        if (rsp.getFacetDates().size() >= 1 && rsp.getFacetDates().get(0).getValues() != null) {
            for(FacetField facetField : rsp.getFacetDates()) {
                List<FacetField.Count> facetFieldValues = facetField.getValues();
                if (facetFieldValues != null) {
                    g.writeStartObject();
                    g.writeStringField("name", facetField.getName());

                    g.writeArrayFieldStart("values");
                    for(FacetField.Count count : facetFieldValues) {
                        if (count.getCount() > 0) {
                            g.writeString(DateUtils.getDateStringFromSolrDate(count.getName()) + " - " +
                                    DateUtils.solrDateMath(count.getName(), count.getFacetField().getGap())
                                    + " (" + count.getCount() + ")");
                        }
                    }
                    g.writeEndArray();
                    g.writeEndObject();
                }
            }
        }
        g.writeEndArray();
    }
}
