package service;

import GatesBigData.utils.*;
import model.*;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.*;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;


public class SearchServiceImpl implements SearchService {

    private static final Logger logger = Logger.getLogger(SearchServiceImpl.class);
    private CoreService coreService;

    @Autowired
    public void setServices(CoreService coreService) {
        this.coreService = coreService;
    }

    private List<String> getFieldNameSubset(Collection<String> fieldNames, boolean isFacetField) {
        List<String> fieldNamesSubset = new ArrayList<String>();

        for(String fieldName : fieldNames) {
            if (SolrUtils.validFieldName(fieldName, isFacetField)) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    private SolrDocument errorDoc(String errorMsg) {
        SolrDocument solrDoc = new SolrDocument();
        solrDoc.addField(Constants.SOLR_ERRORSTRING_KEY, errorMsg);
        return solrDoc;
    }

    public SolrDocument getThumbnailRecord(String coreName, String id) {
        SolrServer server = coreService.getSolrServer(Constants.SOLR_THUMBNAILS_CORE_NAME);
        String queryString = Constants.SOLR_ID_FIELD_NAME + ":\"" + id + "\" AND " +
                             Constants.SOLR_CORE_FIELD_NAME + ":\"" + coreName + "\"";
        SolrDocument doc = getRecord(server, queryString);
        return doc.containsKey(Constants.SOLR_ERRORSTRING_KEY) ? null : doc;
    }

    public SolrDocument getRecord(String coreName, String id) {
        String queryString = Constants.SOLR_ID_FIELD_NAME + ":\"" + id + "\"";
        return getRecord(coreService.getSolrServer(coreName), queryString);
    }

    public SolrDocument getRecord(SolrServer server, String queryString) {
        SolrQuery query = new SolrQuery();
        query.setQuery(queryString);

        try {
            QueryResponse rsp = server.query(query);
            SolrDocumentList results = rsp.getResults();
            if (results.getNumFound() >= 1) {
                return results.get(0);
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }

        return errorDoc("Query " + queryString + " returned no results.");
    }


    public void printRecord(String coreName, String id, boolean isPreview, StringWriter writer) {
        if (Utils.nullOrEmpty(id)) return;

        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);
            g.writeStartObject();

            if (SolrUtils.shouldHaveThumbnail(id)) {
                printThumbnailRecord(coreName, id, g);
            } else {
                printRecord(coreName, id, isPreview, g);
            }

            g.writeEndObject();
            g.close();
        } catch (JsonGenerationException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }


    private void printRecord(String coreName, String id, boolean isPreview, JsonGenerator g) throws IOException {
        SolrDocument doc = getRecord(coreName, id);
        g.writeObjectFieldStart(Constants.SOLR_CONTENT_FIELD_NAME);

        for(String s : getFieldsToWrite(doc, coreName, isPreview)) {
            if (doc.containsKey(s)) {
                g.writeStringField(s, (String) doc.get(s));
            }
        }

        g.writeEndObject();
    }

    private void printThumbnailRecord(String coreName, String id, JsonGenerator g) throws IOException {
        String base64String = "";

        SolrDocument thumbnailDoc = getThumbnailRecord(coreName, id);
        if (thumbnailDoc != null) {
            String content = SolrUtils.getFieldValue(thumbnailDoc, Constants.SOLR_THUMBNAIL_FIELD_NAME, "");
            base64String = Base64.encodeBase64String(content.getBytes());
        }

        g.writeStringField(Constants.SOLR_URL_FIELD_NAME, id);
        g.writeStringField(Constants.SOLR_CONTENT_TYPE_FIELD_NAME, Constants.IMG_CONTENT_TYPE);
        g.writeStringField(Constants.SOLR_CONTENT_FIELD_NAME, Constants.BASE64_CONTENT_TYPE + "," + base64String);
    }

    private List<String> getFieldsToWrite(SolrDocument originalDoc, String coreName, boolean isPreview) {
        if (isPreview) {
            SolrDocument previewFieldsDoc = getRecord(coreName, Constants.SOLR_PREVIEW_FIELDS_ID_FIELD_NAME);
            if (!Utils.nullOrEmpty(previewFieldsDoc)) {
                String previewFields = SolrUtils.getFieldValue(previewFieldsDoc, Constants.SOLR_PREVIEW_FIELDS_ID_FIELD_NAME, "");

                if (!Utils.nullOrEmpty(previewFields)) {
                    return Arrays.asList(previewFields.split(","));
                }
            }
        }

        return new ArrayList<String>(originalDoc.getFieldNames());
    }

    private GroupResponse getSolrSuggestion(String coreName, String userInput, String prefixField, String fullField) {
        String queryStr = prefixField + ":" + userInput + "*";
        String[] userInputPieces = userInput.split("%20| ");
        if (userInputPieces.length > 1) {
            //queryStr = "%2B" + prefixField + ":" + StringUtils.join(userInputPieces, "%20%2B" + prefixField + ":");
            queryStr = "+" + prefixField + ":" + StringUtils.join(userInputPieces, " +" + prefixField + ":");
        }

        SolrServer server = coreService.getSolrServer(coreName);

        ExtendedSolrQuery query = new ExtendedSolrQuery(queryStr);
        query.addField(Constants.SOLR_SCORE_FIELD_NAME);
        query.addField(fullField);
        query.setGroupingDefaults(fullField);

        try {
            QueryResponse rsp = server.query(query);
            return rsp.getGroupResponse();
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    private SuggestionList getTopNListings(GroupResponse response, String fullField, int n) {
        SuggestionList suggestionList = new SuggestionList();
        List<Group> groups = response.getValues().get(0).getValues();

        for(int i = 0; i < groups.size() && suggestionList.size() < n; i++) {
            SolrDocumentList docList = groups.get(i).getResult();
            if (Utils.nullOrEmpty(docList)) continue;

            SolrDocument doc = docList.get(0);
            long numFound = docList.getNumFound();
            float score = (Float) doc.get("score");
            String text = Utils.getUTF8String((String) doc.get(fullField));
            suggestionList.add(text, fullField, numFound, score);
        }

        return suggestionList;
    }

    public JSONObject suggest(String coreName, String userInput, Map<String, String> fieldMap, int listingsPerField) {
        JSONObject ret = new JSONObject();
        SuggestionList suggestions = new SuggestionList();
        userInput = Utils.encodeQuery(userInput);

        for(Map.Entry<String, String> entry : fieldMap.entrySet()) {
            String prefixField = entry.getKey();
            String fullField = entry.getValue();
            GroupResponse response = getSolrSuggestion(coreName, userInput, prefixField, fullField);
            suggestions.concat(getTopNListings(response, fullField, listingsPerField));
        }

        ret.put("suggestions", suggestions.getSortedFormattedSuggestionJSONArray());
        return ret;
    }

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder, int start, int rows,
                           String fq, FacetFieldEntryList facetFields, String viewFields, SolrCollectionSchemaInfo schemaInfo,
                           StringWriter writer) throws IOException {

        QueryResponse rsp = execQuery(queryString, coreName, sortType, SolrUtils.getSortOrder(sortOrder),
                start, rows, fq, facetFields, viewFields, schemaInfo);
        writeResponse(true, rsp, 0, writer);
    }


    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer) throws IOException {
        QueryResponse rsp = execQuery(Constants.SOLR_QUERY_DEFAULT, coreName, Constants.SOLR_SORT_FIELD_DEFAULT,
                      Constants.SOLR_SORT_ORDER_DEFAULT, Constants.SOLR_START_DEFAULT, 0, null, facetFields, "", null);

        writeResponse(false, rsp, 0, writer);
    }

    public SolrDocument getSolrDocumentByFieldValue(String field, String value, String coreName) {
        QueryResponse rsp = execQuery(field + ":" + value, coreName, Constants.SOLR_SORT_FIELD_DEFAULT,
                Constants.SOLR_SORT_ORDER_DEFAULT, Constants.SOLR_START_DEFAULT, 1, null, null, "", null);

        List<SolrDocument> docs = rsp.getResults();
        if (docs.size() == 1) {
            return docs.get(0);
        }
        return null;
    }

    public QueryResponse execQuery(String queryString, String coreName, String sortField, SolrQuery.ORDER sortOrder,
                                   int start, int rows, String fq, FacetFieldEntryList facetFields, String viewFields,
                                   SolrCollectionSchemaInfo schemaInfo) {
        QueryResponse rsp = null;
        SolrServer server = coreService.getSolrServer(coreName);
        ExtendedSolrQuery query = new ExtendedSolrQuery(queryString);

        query.setStart(start);
        query.setRows(rows);

        if (rows > 0) {
            query.setHighlightDefaults();
            query.setHighlightFields(viewFields);
            query.setHighlightQuery(queryString, fq);
        }

        query.setViewFields(viewFields);

        if (!Utils.nullOrEmpty(facetFields)) {
            query.setFacet(true);
            query.setFacetMissing(true);
            query.setFacetLimit(facetFields.size());

            for(FacetFieldEntry field : facetFields) {
                String fieldName = field.getFieldName();
                String fieldType = field.getFieldType();

                if (fieldType.equals("date") && !field.isMultiValued()) {
                    List<String> dateRange = coreService.getSolrFieldDateRange(coreName, fieldName, DateUtils.SOLR_DATE);
                    query.addDateRangeFacet(fieldName, DateUtils.getDateFromSolrDate(dateRange.get(0)),
                            DateUtils.getDateFromSolrDate(dateRange.get(1)), dateRange.get(2));

                    fq = SolrUtils.editFilterQueryDateRange(fq, fieldName);
                } else {
                    query.addFacetField(fieldName);
                }
            }
        }

        query.setFilterQuery(fq);

        if (schemaInfo == null || schemaInfo.isFieldMultiValued(sortField)) {
            sortField = Constants.SOLR_SORT_FIELD_DEFAULT;
        }
        query.addSortField(sortField, sortOrder);

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


    private void writeResponse(boolean includeSearchResults, QueryResponse rsp, int start, StringWriter writer) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeObjectFieldStart(Constants.SOLR_RESPONSE_KEY);

        if (includeSearchResults) {
            g.writeNumberField("start", start);
            writeSearchResponse(rsp, g);
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

    private void writeSearchResponse(QueryResponse rsp, JsonGenerator g) throws IOException {
        SolrDocumentList docs = rsp.getResults();

        g.writeNumberField(Constants.SEARCH_RESPONSE_NUM_FOUND_KEY, docs.getNumFound());
        g.writeArrayFieldStart(Constants.SEARCH_RESPONSE_NUM_DOCS_KEY);

        for (SolrDocument doc : docs) {
            if (!SolrUtils.isLocalDirectory(doc)) {
                g.writeStartObject();
                for(String fieldName : getFieldNameSubset(doc.getFieldNames(), false)) {
                    String[] subFields = StringUtils.split(fieldName, ".");
                    if (subFields.length > 1) {
                        g.writeObjectFieldStart(subFields[0]);
                        //DateUtils.getFormattedDateStringFromSolrDate(doc.getString(dateFieldString), DateUtils.LONG_DATE));
                        Utils.writeValueByType(subFields[1], doc.getFieldValue(fieldName), g);
                        g.writeEndObject();
                    } else {
                        Utils.writeValueByType(fieldName, doc.getFieldValue(fieldName), g);
                    }
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
