package service;


import model.FacetFieldEntryList;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.TreeMap;

public interface SearchService {

    public FacetFieldEntryList getFacetFieldsFromLuke(String coreName, boolean facetFields);

    public JSONObject suggest(String coreName, String userInput, String fieldSpecificEndpoint);

    public QueryResponse execQuery(String queryString, String coreName, String sortType, SolrQuery.ORDER sortOrder,
                                   int start, int rows, String fq, FacetFieldEntryList facetFields) throws IOException;

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer);

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, int rows, String fq, FacetFieldEntryList facetFields, StringWriter writer) throws IOException;
}

