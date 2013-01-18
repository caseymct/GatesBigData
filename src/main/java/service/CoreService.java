package service;

import org.apache.nutch.parse.ParseData;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.List;

public interface CoreService {

    public SolrServer getCloudSolrServer(String coreName);

    public SolrServer getSolrServer(String collectionName);

    public SolrServer getHttpSolrServer(String coreName);

    public boolean createAndAddDocumentToSolr(Object content, String hdfsKey, String coreName);

    public boolean addInfoFilesToSolr(String coreName, HashMap<String, String> hdfsInfoFileContents);

    public boolean addDocumentToSolrIndex(SolrInputDocument doc, String coreName);

    public boolean addDocumentsToSolrIndex(List<SolrInputDocument> docs, String coreName);

    public SolrInputDocument createSolrDocument(HashMap<String, String> params);

    public SolrInputDocument createSolrInputDocumentFromNutch(String urlString, ParseData parseData, String segment,
                                                              String coreName, String contentType, String content);

    public boolean addNutchDocumentToSolr(String urlString, ParseData parseData, String segment,
                                          String coreName, String contentType, String content);

    public boolean deleteIndex(String coreName);

    public boolean deleteById(String coreName, List<String> ids);

    public boolean deleteByField(String coreName, String field, String value);

    public boolean deleteByField(String coreName, String field, List<String> values);

    public List<String> getSolrFieldDateRange(String coreName, String field, String format);
}
