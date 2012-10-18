package service;


import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;

import java.util.List;

public interface CoreService {

    public SolrServer getSolrServer(String coreName);

    public List<String> getFieldNamesFromLuke(String coreName);

    public boolean isFieldMultiValued(SolrServer server, String fieldName);

    public boolean addDocumentToSolr(Object content, String hdfsKey, String coreName);

    public boolean deleteIndex(String coreName);

    public JSONObject getCoreData(String coreName);

    public Object getCoreDataIndexProperty(String coreName, String property);

}
