package service;


import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.List;

public interface SearchService {

    public List<String> getSolrIndexDateRange(String collectionName);

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, String fq, JsonGenerator g) throws IOException;
}

