package service;


import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.TreeMap;

public interface SearchService {

    public List<String> getSolrIndexDateRange(String collectionName);

    public JSONObject suggest(String userInput, String fieldSpecificEndpoint);

    public QueryResponse execQuery(String queryString, String coreName, String sortType, String sortOrder,
                                   int start, int rows, String fq, TreeMap<String, String> facetFields) throws IOException;

    public void writeFacets(String coreName, TreeMap<String, String> facetFields, StringWriter writer);

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, int rows, String fq, TreeMap<String,String> facetFields, StringWriter writer) throws IOException;
}

