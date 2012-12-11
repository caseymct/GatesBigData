package service;


import model.FacetFieldEntryList;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

public interface SearchService {

    public JSONObject suggest(String coreName, String userInput, HashMap<String, String> fieldMap, int entriesPerField);

    public JSONObject execQuery(String queryString, String coreName, String sortType, SolrQuery.ORDER sortOrder,
                                   int start, int rows, String fq, FacetFieldEntryList facetFields, String viewFields) throws IOException;

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer);

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder, int start, int rows,
                           String fq, FacetFieldEntryList facetFields, String viewFields, List<String> dateFields, StringWriter writer) throws IOException;

    public SolrDocument getRecord(String coreName, String id);

    public void printRecord(String coreName, String id, StringWriter writer);

    public void printRecord(String coreName, String id, StringWriter writer, List<String> previewFields);

    public void printRecord(SolrDocument doc, StringWriter writer, List<String> fields);
}

