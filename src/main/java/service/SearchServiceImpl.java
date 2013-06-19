package service;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.*;
import GatesBigData.utils.*;
import model.*;
import net.sf.json.JSONArray;
import org.apache.commons.net.util.Base64;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
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

        if (Solr.isAuditViewOrPreview(viewType)) {
            JSONArray fieldsStr = getCollectionInfoFieldValuesAsJSONArray(coreName, Solr.getInfoField(viewType));

            if (!Utils.nullOrEmpty(fieldsStr)) {
                List<String> fields = JSONUtils.convertJSONArrayToStringList(fieldsStr);
                Collections.sort(fields);
                return fields;
            }
        }

        return SolrUtils.getAllViewFields(originalDoc);
    }

    public SolrDocumentList getResultList(String collection, String queryStr, String fq, String sortField, SolrQuery.ORDER sortOrder,
                                          Integer start, Integer rows, String viewFields) {
        return getResultList(buildQuery(queryStr, fq, sortField, sortOrder, start, rows, viewFields), collection);
    }

    public SolrDocumentList getResultList(String collection, String queryStr, String fq, List<SolrQuery.SortClause> sortClauses,
                                          Integer start, Integer rows, String viewFields) {
        return getResultList(buildQuery(queryStr, fq, sortClauses, start, rows, viewFields), collection);
    }

    public SolrDocumentList getResultList(ExtendedSolrQuery query, String collection) {
        QueryResponse rsp = execQuery(query, collection);
        return rsp != null ? rsp.getResults() : new SolrDocumentList();
    }

    public SolrDocument getRecordById(String collection, String id) {
        return getRecord(collection, FieldNames.ID, id);
    }

    public SolrDocument getRecord(String collection, String field, String value) {
        return getRecord(buildSingleResultQuery(field + ":" + value, null), collection);
    }

    public SolrDocument getRecord(String collection, HashMap<String, String> queryParams, String op) {
        return getRecord(new ExtendedSolrQuery(queryParams, op), collection);
    }

    public SolrDocument getRecord(ExtendedSolrQuery query, String collection) {
        SolrDocumentList docs = getResultList(query, collection);
        return docs.size() >= 1 ? docs.get(0) : null;
    }

    public String getCollectionInfoFieldValue(String collection, String fieldName) {
        fieldName = fieldName.toUpperCase();
        if (!FieldNames.isInfoField(fieldName)) {
            return "";
        }

        SolrDocument doc = getRecord(collection, FieldNames.TITLE, fieldName);
        return SolrUtils.getFieldStringValue(doc, fieldName, "").trim();
    }

    public JSONArray getCollectionInfoFieldValuesAsJSONArray(String collection, String fieldName) {
        return JSONUtils.convertStringToJSONArray(getCollectionInfoFieldValue(collection, fieldName));
    }

    public List<String> getCollectionInfoFieldValues(String collection, String fieldName) {
        JSONArray j = JSONUtils.convertStringToJSONArray(getCollectionInfoFieldValue(collection, fieldName));
        return JSONUtils.convertJSONArrayToStringList(j);
    }

    /* Combine preview fields and table fields
     */
    public Set<String> getViewFields(String collection) {
        Set<String> fieldList = new HashSet<String>();
        fieldList.addAll(getCollectionInfoFieldValues(collection, FieldNames.PREVIEW_FIELDS));
        fieldList.addAll(getCollectionInfoFieldValues(collection, FieldNames.TABLE_FIELDS));
        fieldList.addAll(Arrays.asList(FieldNames.ID, FieldNames.URL));
        return fieldList;
    }

    private String getThumbnailRecord(String coreName, String id) {
        HashMap<String, String> queryParams = new HashMap<String, String>();
        queryParams.put(FieldNames.ID, id);
        queryParams.put(FieldNames.CORE, coreName);

        SolrDocument doc = getRecord(Solr.THUMBNAILS_COLLECTION_NAME, queryParams, Operations.BOOLEAN_UNION);
        Object thumbnail = SolrUtils.getFieldValue(doc, FieldNames.THUMBNAIL);

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
        SolrDocument doc = getRecordById(coreName, id);
        g.writeObjectFieldStart(FieldNames.CONTENT);

        String currParent = "";
        boolean writingSubObj = false;

        for(String field : getFieldsToWrite(doc, coreName, viewType)) {
            String val;
            if (!doc.containsKey(field)) {
                val = "<i>Field does not exist in this database</i>";

            } else {
                val = doc.get(field).toString();

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
        Utils.writeValueByType(FieldNames.URL, id, g);
        Utils.writeValueByType(FieldNames.CONTENT_TYPE, Constants.IMG_CONTENT_TYPE, g);
        g.writeStringField(FieldNames.CONTENT, thumbnailRecord);
    }

    private SuggestionList getTopNListings(GroupResponse response, String fullField, int n) {
        SuggestionList suggestionList = new SuggestionList();
        List<Group> groups = response.getValues().get(0).getValues();

        for(int i = 0; i < groups.size() && suggestionList.size() < n; i++) {
            SolrDocumentList docList = groups.get(i).getResult();
            if (docList.size() == 0) continue;

            SolrDocument doc = docList.get(0);
            long numFound = docList.getNumFound();
            float score = (Float) doc.get(FieldNames.SCORE);
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
            String viewFields  = FieldNames.SCORE + "," + fullField;

            GroupResponse rsp = getGroupResponse(coreName, queryStr, null, Defaults.ROWS, viewFields,
                    fullField, Defaults.SUGGESTION_GROUP_LIMIT);
            if (rsp == null) continue;

            suggestions.concat(getTopNListings(rsp, fullField, listingsPerField));
        }

        return suggestions.getSortedFormattedSuggestionJSONArray();
    }

    public JSONArray suggestUsingSeparateCore(String coreName, String userInput, int n) {
        SuggestionList suggestions = new SuggestionList();

        String queryString = SolrUtils.getSuggestionQuery(FieldNames.CONTENT, userInput);
        SolrDocumentList docs = getResultList(buildSuggestQuery(queryString, n), coreName);

        int listMaxSize = Math.min(docs.size(), n);

        for(int i = 0; i < listMaxSize && suggestions.size() < listMaxSize; i++) {
            SolrDocument doc = docs.get(i);
            String field = SolrUtils.getRelevantSuggestionField(doc);
            String fieldValue = SolrUtils.getFieldStringValue(doc, field, "");
            long count = (Long) doc.getFieldValue(FieldNames.COUNT);

            if (!fieldValue.equals("") && count > 0) {
                suggestions.add(fieldValue, FieldNames.getFacetDisplayName(field), count, 1.0);
            }
        }

        return suggestions.getFormattedSuggestionJSONArray();
    }

    public QueryResponse findSearchResults(String collection, String queryString, String sortField, String sortOrder, Integer start, Integer rows,
                                           String fq, FacetFieldEntryList facetFields, String viewFields, boolean includeHighlighting,
                                           SolrCollectionSchemaInfo info) {
        return findSearchResults(collection, queryString, SolrUtils.createSortClauseList(sortField, sortOrder), start, rows, fq,
                facetFields, viewFields, includeHighlighting, info);
    }

    public QueryResponse findSearchResults(String collection, String queryString, List<SolrQuery.SortClause> sortClauses, Integer start, Integer rows,
                                           String fq, FacetFieldEntryList facetFields, String fl, boolean includeHighlighting,
                                           SolrCollectionSchemaInfo info) {
        ExtendedSolrQuery query = buildQuery(queryString, fq, sortClauses, start, rows, fl);
        query.addQueryFacets(facetFields, info);
        if (includeHighlighting) {
            query.setHighlightDefaults();
        }

        return execQuery(query, collection);
    }

    public Map<String, FieldStatsInfo> getStatsResults(String collection, String queryStr, String fq, List<String> statsFields) {
        QueryResponse rsp = execQuery(buildStatsQuery(queryStr, fq, statsFields), collection);
        return rsp != null ? rsp.getFieldStatsInfo() : new HashMap<String, FieldStatsInfo>();
    }

    public void findAndWriteSearchResults(String collection, String query, List<SolrQuery.SortClause> sortClauses, Integer start, Integer rows,
                                          String fq, FacetFieldEntryList facetFields, String fl, boolean includeHighlighting,
                                          SolrCollectionSchemaInfo info, StringWriter writer) throws IOException {

        QueryResponse rsp = findSearchResults(collection, query, sortClauses, start, rows, fq, facetFields,
                fl, includeHighlighting, info);

        JsonGenerator g = writeSearchResponseStart(writer, query);
        writeSearchResponse(rsp, info.getDateFieldNames(), g);

        if (includeHighlighting) {
            writeHighlightResponse(rsp, g);
        }
        writeFacetResponse(rsp, g);
        writeSearchResponseEnd(g);
    }

    public List<String> getFieldCounts(String coreName, String queryString, String fq, List<String> facetFields, boolean separateFacetCount)
            throws IOException {

        ExtendedSolrQuery query = buildCountsPerFieldQuery(queryString, fq, facetFields, separateFacetCount);
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

    public SeriesPlot getPlotData(String collection, String queryString, String fq, int numFound, String xAxisField,
                                  boolean xAxisIsDate, String yAxisField, boolean yAxisIsDate, String seriesField,
                                  boolean seriesIsDate, Integer maxPlotPoints, String dateRangeGap) {

        ExtendedSolrQuery query = buildPlotQuery(collection, queryString, fq, numFound, xAxisField, yAxisField, seriesField,
                                                 maxPlotPoints, dateRangeGap);
        SolrDocumentList docs = getResultList(query, collection);

        return new SeriesPlot(xAxisField, xAxisIsDate, yAxisField, yAxisIsDate, seriesField, seriesIsDate, docs);
    }

    public void findAndWriteInitialFacetsFromSuggestionCore(String collection, List<String> fields, StringWriter writer)
        throws IOException {

        JsonGenerator g = writeSearchResponseStart(writer, null);
        g.writeArrayFieldStart(Response.FACET_KEY);

        for(String field : fields) {
            ExtendedSolrQuery query = buildSuggestionCoreFieldQuery(field);
            QueryResponse rsp = execQuery(query, collection);
            writeSuggestionResponse(field, rsp, g);
        }

        g.writeEndArray();
        writeSearchResponseEnd(g);
    }

    public void findAndWriteFacets(String collection, FacetFieldEntryList facetFields, SolrCollectionSchemaInfo info,
                                   StringWriter writer) throws IOException {
        findAndWriteFacets(collection, Defaults.QUERY, null, facetFields, info, writer);
    }

    public void findAndWriteFacets(String collection, String queryStr, String fq, FacetFieldEntryList facetFields,
                                   SolrCollectionSchemaInfo info, StringWriter writer) throws IOException {
        findAndWriteFacets(collection, queryStr, fq, facetFields, null, info, writer);
    }

    public void findAndWriteFacets(String collection, String queryStr, String fq, FacetFieldEntryList facetFields,
                                   Map<String, Object> additionalFields, SolrCollectionSchemaInfo info,
                                   StringWriter writer) throws IOException {
        ExtendedSolrQuery query = buildFacetOnlyQuery(queryStr, fq, facetFields, info);
        QueryResponse rsp = execQuery(query, collection);

        JsonGenerator g = writeSearchResponseStart(writer, query.toString());
        writeFacetResponse(rsp, g);
        writeAdditionalFields(additionalFields, g);
        writeSearchResponseEnd(g);
    }

    public GroupResponse getGroupResponse(String collection, String queryStr, String fq, Integer rows, String fl,
                                          String groupField, int groupLimit) {
        QueryResponse rsp = execQuery(buildGroupQuery(queryStr, fq, rows, fl, groupField, groupLimit), collection);
        return rsp != null ? rsp.getGroupResponse() : null;
    }

    private ExtendedSolrQuery buildDateQuery(String field, boolean fieldIsDate, SolrQuery.ORDER order) {
        String fq = fieldIsDate ? field + ":[NOW-100YEARS TO NOW]" : null;  //If this is a date field, make sure we get reasonable responses
        return buildQuery(Defaults.QUERY, fq, field, order, 0, 1, field);
    }

    private List<Object> getFieldRangeForQuery(String collection, String queryStr, String fq, String field) {
        List<Object> range = new ArrayList<Object>();

        for(SolrQuery.ORDER order : Arrays.asList(SolrQuery.ORDER.asc, SolrQuery.ORDER.desc)) {
            ExtendedSolrQuery query = buildQuery(queryStr, fq, field, order, 0, 1, field);

            SolrDocument doc = getRecord(query, collection);
            if (doc != null) {
                range.add(SolrUtils.getFieldValue(doc, field));
            }
        }
        return range;
    }

    private int getNGroups(String collection, String queryStr, String fq, String field) {
        ExtendedSolrQuery query = buildQuery(queryStr, fq, null, 0, 0, null);
        query.setGroup(true);
        query.setGroupField(field);
        query.showGroupNGroups();

        GroupResponse rsp = execQuery(query, collection).getGroupResponse();
        if (rsp != null) {
            List<GroupCommand> values = rsp.getValues();
            if (values != null && values.size() > 0) {
                return values.get(0).getNGroups();
            }
        }
        return -1;
    }

    private ExtendedSolrQuery buildPlotQuery(String collection, String queryStr, String fq, int numFound, String xAxisField, String yAxisField,
                                             String seriesField, Integer maxPlotPoints, String dateRangeGap) {
        String fl = yAxisField + (!Utils.nullOrEmpty(seriesField) ? "," + seriesField : "");

        List<SolrQuery.SortClause> clauses = new ArrayList<SolrQuery.SortClause>();
        clauses.add(new SolrQuery.SortClause(xAxisField, SolrQuery.ORDER.asc));
        clauses.add(new SolrQuery.SortClause(yAxisField, SolrQuery.ORDER.asc));

        ExtendedSolrQuery query = buildQuery(queryStr, fq, clauses, 0, numFound, fl);
        query.setGroup(true);
        query.showGroupNGroups();

        int nGroups = getNGroups(collection, queryStr, fq, xAxisField);

        if (maxPlotPoints != null && nGroups > maxPlotPoints) {
            List<Object> range = getFieldRangeForQuery(collection, queryStr, fq, xAxisField);
            double min = Double.parseDouble(range.get(0).toString());

            // group.query=INVOICEAPPLIEDAMOUNT:[-200.0%20TO%20200.0]
            // group.query=INVOICEAPPLIEDAMOUNT:[201.0%20TO%20*]

        } else {
            query.setGroupField(xAxisField);
        }

        return query;
    }

    private ExtendedSolrQuery buildCountsPerFieldQuery(String queryStr, String fq, List<String> facetFields,
                                                       boolean separateFacetCounts) {
        ExtendedSolrQuery query = buildQuery(queryStr, fq, null, 0, 0, null);
        query.setFacet(true);
        query.setFacetMissing(!separateFacetCounts);
        query.setFacetLimit(separateFacetCounts ? -1 : 1);
        query.setFacetMinCount(separateFacetCounts ? 1 : Integer.MAX_VALUE);
        query.addFacetFields(facetFields);
        return query;
    }

    private ExtendedSolrQuery buildSingleResultQuery(String queryStr, String field) {
        return buildQuery(queryStr, null, null, 0, 1, field);
    }

    private ExtendedSolrQuery buildFacetOnlyQuery(String queryStr, String fq, FacetFieldEntryList facetFields,
                                                  SolrCollectionSchemaInfo info) {
        ExtendedSolrQuery query = buildQuery(queryStr, fq, null, 0, 0, null);
        query.addQueryFacets(facetFields, info);
        return query;
    }

    private ExtendedSolrQuery buildSuggestQuery(String queryStr, Integer rows) {
        List<SolrQuery.SortClause> clauses = new ArrayList<SolrQuery.SortClause>();
        clauses.add(new SolrQuery.SortClause(FieldNames.SCORE, SolrQuery.ORDER.desc));
        clauses.add(new SolrQuery.SortClause(FieldNames.COUNT, SolrQuery.ORDER.desc));
        return buildQuery(queryStr, null, clauses, 0, rows, null);
    }

    private ExtendedSolrQuery buildStatsQuery(String queryStr, String fq, List<String> statsFields) {
        ExtendedSolrQuery query = buildQuery(queryStr, fq, null, 0, 0, null);
        query.setStatsFields(statsFields);
        return query;
    }

    private ExtendedSolrQuery buildRecordExistsQuery(HashMap<String, String> fqList) {
        ExtendedSolrQuery query = new ExtendedSolrQuery(Defaults.QUERY);
        query.setRows(0);

        if (!Utils.nullOrEmpty(fqList)) {
            for(Map.Entry<String, String> fqEntry : fqList.entrySet()) {
                boolean nullValue = fqEntry.getValue() == null;
                String fieldName  = (nullValue ? "-" : "+") + fqEntry.getKey();
                String fieldValue = nullValue ? "[* TO *]" : "\"" + fqEntry.getValue() + "\"";
                query.addFilterQuery(fieldName + ":" + fieldValue);
            }
        }
        return query;
    }

    private ExtendedSolrQuery buildRecordExistsQuery(String id) {
        ExtendedSolrQuery query = new ExtendedSolrQuery(FieldNames.ID + ":" + id);
        query.setRows(0);
        return query;
    }

    private ExtendedSolrQuery buildSuggestionCoreFieldQuery(String field) {
        List<SolrQuery.SortClause> sortClauses = new ArrayList<SolrQuery.SortClause>();
        sortClauses.add(new SolrQuery.SortClause(FieldNames.COUNT, SolrQuery.ORDER.desc));
        sortClauses.add(new SolrQuery.SortClause(field, SolrQuery.ORDER.desc));
        String fl = field + "," + FieldNames.COUNT;

        return buildQuery(field + ":*", null, sortClauses, 0, Defaults.MAX_FACET_RESULTS, fl);
    }

    private ExtendedSolrQuery buildGroupQuery(String queryStr, String fq, Integer rows, String fl,
                                              String groupField, int groupLimit) {
        ExtendedSolrQuery query = buildQuery(queryStr, fq, null, 0, rows, fl);
        query.setGroupingDefaults(groupField);
        //query.setGroupLimit(groupLimit);

        return query;
    }

    private ExtendedSolrQuery buildQuery(String queryString, String fq, String sortField, SolrQuery.ORDER sortOrder,
                                         Integer start, Integer rows, String fl) {
        return buildQuery(queryString, fq, SolrUtils.createSortClauseList(sortField, sortOrder), start, rows, fl);
    }

    private ExtendedSolrQuery buildQuery(String queryString, String fq, List<SolrQuery.SortClause> sortClauses,
                                         Integer start, Integer rows, String fl) {

        ExtendedSolrQuery query = new ExtendedSolrQuery(queryString);
        query.setQueryStart(start);
        query.setQueryRows(rows);
        query.setViewFields(fl);
        query.addSortClauses(sortClauses);
        query.setFilterQuery(fq);
        return query;
    }

    public List<Date> getSolrFieldDateRange(String collection, String field, SolrCollectionSchemaInfo info) {
        if (info.hasDynamicDateFieldDateRanges()) {
            return info.getDateRange(field);
        }

        List<Date> dates = new ArrayList<Date>();
        boolean isDate = info.fieldTypeIsDate(field);

        for(SolrQuery.ORDER order : Arrays.asList(SolrQuery.ORDER.asc, SolrQuery.ORDER.desc)) {
            SolrDocument doc = getRecord(buildDateQuery(field, isDate, order), collection);
            dates.add(SolrUtils.getFieldDateValue(doc, field));
        }
        return dates;
    }

    public QueryResponse execQuery(ExtendedSolrQuery query, String collection) {
        try {
            return coreService.getSolrServer(collection).query(query);
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public JsonGenerator writeSearchResponseStart(StringWriter writer, String queryString) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeObjectFieldStart(Response.RESPONSE_KEY);
        if (queryString != null) {
            g.writeStringField(QueryParams.QUERY, queryString);
        }
        return g;
    }

    public void writeSearchResponseEnd(JsonGenerator g) throws IOException {
        g.writeEndObject();
        g.close();
    }

    private void writeAdditionalFields(Map<String, Object> additionalFields, JsonGenerator g) throws IOException {
        if (Utils.nullOrEmpty(additionalFields)) {
            return;
        }

        for(Map.Entry<String, Object> entry : additionalFields.entrySet()) {
            Utils.writeValueByType(entry.getKey(), entry.getValue(), g);
        }
    }

    private void writeHighlightResponse(QueryResponse rsp, JsonGenerator g) throws IOException {
        Map<String, Map<String, List<String>>> highlighting = rsp.getHighlighting();
        if (highlighting == null) return;

        NamedList header = rsp.getHeader();

        g.writeObjectFieldStart(Response.HIGHLIGHTING_KEY);

        NamedList params = (NamedList) header.get(Response.HEADER_PARAMS_KEY);
        g.writeStringField(QueryParams.HIGHLIGHT_PRE, (String) params.get(QueryParams.HIGHLIGHT_PRE));
        g.writeStringField(QueryParams.HIGHLIGHT_POST, (String) params.get(QueryParams.HIGHLIGHT_POST));

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

        g.writeStringField(QueryParams.FIELDLIST, (String) ((SimpleOrderedMap) rsp.getHeader().get(Response.HEADER_PARAMS_KEY)).get(QueryParams.FIELDLIST));
        g.writeNumberField(Response.NUM_FOUND_KEY, docs.getNumFound());
        g.writeArrayFieldStart(Response.DOCS_KEY);

        for (SolrDocument doc : docs) {
            if (!SolrUtils.isLocalDirectory(doc)) {
                g.writeStartObject();
                for(String fieldName : SolrUtils.getValidFieldNamesSubset(doc.getFieldNames())) {
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

    private void writeFieldStats(QueryResponse rsp, JsonGenerator g) throws IOException {
        g.writeArrayFieldStart(Response.STATS_KEY);

        for (Map.Entry<String, FieldStatsInfo> entry : rsp.getFieldStatsInfo().entrySet()) {
            FieldStatsInfo info = entry.getValue();

            g.writeStartObject();
            Utils.writeValueByType("name", info.getName(), g);
            Utils.writeValueByType("min", info.getMin(), g);
            Utils.writeValueByType("max", info.getMax(), g);
            Utils.writeValueByType("count", info.getCount(), g);
            Utils.writeValueByType("missing", info.getMissing(), g);
            Utils.writeValueByType("sum", info.getSum(), g);
            Utils.writeValueByType("mean", info.getMean(), g);
            Utils.writeValueByType("stddev", info.getStddev(), g);
            g.writeEndObject();
        }
        g.writeEndArray();
    }

    private void writeFacets(List<FacetField> facetFieldList, boolean isDate, JsonGenerator g) throws IOException {
        if (Utils.nullOrEmpty(facetFieldList)) return;

        for(FacetField facetField : facetFieldList) {
            List<FacetField.Count> facetFieldValues = facetField.getValues();
            if (facetFieldValues == null) continue;

            g.writeStartObject();
            g.writeStringField(Response.NAME_KEY, FieldNames.getFacetDisplayName(facetField.getName()));

            g.writeArrayFieldStart(Response.VALUES_KEY);
            for(FacetField.Count count : facetFieldValues) {
                String facetFieldDisplayStr = SolrUtils.getFacetFieldDisplayString(count, isDate);
                if (Utils.nullOrEmpty(facetFieldDisplayStr)) continue;

                g.writeString(facetFieldDisplayStr);
            }
            g.writeEndArray();
            g.writeEndObject();
        }
    }

    private void writeSuggestionResponse(String field, QueryResponse rsp, JsonGenerator g) throws IOException {
        SolrDocumentList docs = rsp.getResults();

        g.writeStartObject();
        g.writeStringField(Response.NAME_KEY, field);
        g.writeArrayFieldStart(Response.VALUES_KEY);

        for (SolrDocument doc : docs) {
            g.writeString(doc.getFieldValue(field) + " (" + doc.getFieldValue(FieldNames.COUNT) + ")");
        }
        g.writeEndArray();
        g.writeEndObject();
    }

    private void writeFacetResponse(QueryResponse rsp, JsonGenerator g) throws IOException {
        g.writeArrayFieldStart(Response.FACET_KEY);
        writeFacets(rsp.getFacetFields(), false, g);
        writeFacets(rsp.getFacetDates(), true, g);
        g.writeEndArray();
    }
}
