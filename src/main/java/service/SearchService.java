package service;


import model.ExtendedSolrQuery;
import model.FacetFieldEntryList;
import model.SeriesPlot;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

public interface SearchService {

    public List<String> getFieldsToWrite(SolrDocument originalDoc, String coreName, String viewType);

    public List<String> getSolrFieldDateRange(String coreName, String field, String format);

    public JSONArray suggestUsingSeparateCore(String coreName, String userInput, int n);

    public JSONArray suggestUsingGrouping(String coreName, String userInput, Map<String, String> fieldMap, int listingsPerField);

    public ExtendedSolrQuery buildQuery(String coreName, String queryString, String fq, String sortField,
                                        SolrQuery.ORDER sortOrder, int start, int rows, FacetFieldEntryList facetFields,
                                        String viewFields);

    public ExtendedSolrQuery buildHighlightQuery(String coreName, String queryString, String fq, String sortField,
                                                 SolrQuery.ORDER sortOrder, int start, int rows, FacetFieldEntryList facetFields,
                                                 String viewFields);

    public QueryResponse execQuery(ExtendedSolrQuery query, String coreName);

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder, int start, int rows,
                           String fq, FacetFieldEntryList facetFields, String viewFields, List<String> dateFields,
                           StringWriter writer) throws IOException;

    public SeriesPlot getPlotData(String coreName, String queryString, String fq, int numFounds, String xAxisField,
                                  boolean xAxisIsDate, String yAxisField, boolean yAxisIsDate, String seriesField,
                                  boolean seriesIsDate);

    public SolrDocumentList getResultList(String coreName, String queryStr, String fq, String sortField, SolrQuery.ORDER sortOrder,
                                          int start, int rows, String viewFields);

    public SolrDocumentList getResultList(ExtendedSolrQuery query, String coreName);

    public GroupResponse getGroupResponse(String coreName, String queryStr, String fq, int rows, String viewFields,
                                       String groupField, int groupLimit);

    public SolrDocument getRecord(String coreName, String id);

    public SolrDocument getRecord(String coreName, HashMap<String, String> queryParams, String booleanOperator);

    public SolrDocument getRecord(ExtendedSolrQuery query, String coreName);

    public SolrDocument getRecordByFieldValue(String field, String value, String coreName);

    public List<String> getFieldCounts(String coreName, String queryString, String fq, List<String> facetFields,
                                       boolean separateFacetCount) throws IOException;

    // get value of facetFields, viewFields, tableFields, etc
    public String getCoreInfoFieldValue(String coreName, String fieldName);

    public String getRecordCoreTitle(String coreName);

    public void printRecord(String coreName, String id, String viewType, boolean isStructuredData, StringWriter writer);

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer) throws IOException;
}

