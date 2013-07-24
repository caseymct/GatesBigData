package service;

import model.schema.CollectionSchemaInfo;
import model.search.ExtendedSolrQuery;
import model.search.FacetFieldEntryList;
import model.analysis.SeriesPlot;
import net.sf.json.JSONArray;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

public interface SearchService {

    public List<Date> getSolrFieldDateRange(String collection, String field, CollectionSchemaInfo schemaInfo);

    public JSONArray suggestUsingSeparateCore(String coreName, String userInput, int n);

    public JSONArray suggestUsingGrouping(String coreName, String userInput, Map<String, String> fieldMap, int listingsPerField);


    public SeriesPlot getPlotData(String coreName, String queryString, String fq, int numFounds, String xAxisField,
                                  boolean xAxisIsDate, String yAxisField, boolean yAxisIsDate, String seriesField,
                                  boolean seriesIsDate, Integer maxPlotPoints, String dateRangeGap);

    public SolrDocumentList getResultList(String coreName, String queryStr, String fq, String sortField, SolrQuery.ORDER sortOrder,
                                          Integer start, Integer rows, String viewFields);

    public SolrDocumentList getResultList(String coreName, String queryStr, String fq, List<SolrQuery.SortClause> sortClauses,
                                          Integer start, Integer rows, String viewFields);

    public SolrDocumentList getResultList(ExtendedSolrQuery query, String collection);

    public GroupResponse getGroupResponse(String coreName, String queryStr, String fq, Integer rows, String viewFields,
                                       String groupField, int groupLimit);

    public boolean recordExists(String coreName, HashMap<String, String> fqList);

    public boolean recordExists(String coreName, String id);

    public boolean recordExists(String coreName, ExtendedSolrQuery query);

    public SolrDocument getRecordById(String coreName, String id);

    public SolrDocument getRecord(String collection, String field, String value);

    public SolrDocument getRecord(String collection, HashMap<String, String> queryParams, String booleanOperator);

    public SolrDocument getRecord(ExtendedSolrQuery query, String coreName);

    public List<String> getFieldCounts(String coreName, String queryString, String fq, List<String> facetFields,
                                       boolean separateFacetCount) throws IOException;

    public void printRecord(String collection, String id, List<String> fields, boolean isStructuredData, StringWriter writer);


    public QueryResponse findSearchResults(String collection, String queryString, String sortField, String sortOrder, Integer start, Integer rows,
                                           String fq, FacetFieldEntryList facetFields, String viewFields, boolean includeHighlighting,
                                           CollectionSchemaInfo info);

    /* Write search response methods */
    public void findAndWriteSearchResults(String collection, String query, List<SolrQuery.SortClause> sortClauses, Integer start, Integer rows,
                                          String fq, FacetFieldEntryList facetFields, String fl, boolean includeHighlighting,
                                          CollectionSchemaInfo info, StringWriter writer) throws IOException;

    public void findAndWriteInitialFacetsFromSuggestionCore(String collection, List<String> fields, StringWriter writer)
            throws IOException;

    public void findAndWriteFacets(String collection, FacetFieldEntryList facetFields, CollectionSchemaInfo info,
                                   StringWriter writer) throws IOException;

    public void findAndWriteFacets(String collection, String queryStr, String fq, FacetFieldEntryList facetFields,
                                   CollectionSchemaInfo info, StringWriter writer) throws IOException;

    public void findAndWriteFacets(String collection, String queryStr, String fq, FacetFieldEntryList facetFields,
                                   Map<String, Object> additionalFields, CollectionSchemaInfo info, StringWriter writer) throws IOException;

    public Map<String, FieldStatsInfo> getStatsResults(String collection, String queryStr, String fq, List<String> statsFields);

    public JsonGenerator writeSearchResponseStart(StringWriter writer, String queryString) throws IOException;

    public void writeSearchResponseEnd(JsonGenerator g) throws IOException;
}

