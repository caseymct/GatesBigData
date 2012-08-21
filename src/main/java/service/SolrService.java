package service;


import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.List;

public interface SolrService {

    public List<String> getSolrIndexDateRange(String collectionName);

    public void solrSearch(String collectionName, String queryString, String sortType, String sortOrder,
                           int start, String fq, JsonGenerator g) throws IOException;

    public String importCsvFileOnLocalSystemToSolr(String collectionName, String fileName);

    public String importCsvFileOnServerToSolr(String collectionName, String fileName);

}
