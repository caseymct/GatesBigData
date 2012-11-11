package service;

import net.sf.json.JSONObject;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.util.List;

public interface CoreService {

    public SolrServer getSolrServer(String coreName);

    public boolean isFieldMultiValued(SolrServer server, String fieldName);

    public boolean fieldExists(SolrServer server, String fieldName);

    public boolean createAndAddDocumentToSolr(Object content, String hdfsKey, String coreName);

    public boolean addDocumentToSolrIndex(SolrInputDocument doc, String coreName);

    public boolean addDocumentToSolrIndex(List<SolrInputDocument> docs, String coreName);

    public SolrInputDocument createSolrInputDocumentFromNutch(String urlString, ParseData parseData, String segment,
                                                              String coreName, String contentType, Content content);

    public boolean addNutchDocumentToSolr(String urlString, ParseData parseData, String segment,
                                          String coreName, String contentType, Content content);

    public boolean deleteIndex(String coreName);

    public JSONObject getCoreData(String coreName);

    public Object getCoreDataIndexProperty(String coreName, String property);

    public List<String> getSolrFieldDateRange(String coreName, String field, String format);
}
