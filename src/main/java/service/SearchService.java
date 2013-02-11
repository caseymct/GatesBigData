package service;


import model.FacetFieldEntryList;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

public interface SearchService {

    public JSONObject suggest(String coreName, String userInput, Map<String, String> fieldMap, int entriesPerField);

    public SolrDocument getSolrDocumentByFieldValue(String field, String value, String coreName);

    public QueryResponse execQuery(String queryString, String coreName, String sortType, SolrQuery.ORDER sortOrder,
                                    int start, int rows, String fq, FacetFieldEntryList facetFields, String viewFields,
                                    SolrCollectionSchemaInfo schemaInfo) throws IOException;

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer) throws IOException;

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder, int start, int rows,
                           String fq, FacetFieldEntryList facetFields, String viewFields, SolrCollectionSchemaInfo schemaInfo,
                           StringWriter writer) throws IOException;

    public SolrDocument getThumbnailRecord(String coreName, String id);

    public SolrDocument getRecord(String coreName, String id);

    public SolrDocument getRecord(SolrServer server, String queryString);

    public void printRecord(String coreName, String id, boolean isPreview, StringWriter writer);
}

