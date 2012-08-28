package service;


import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.util.NamedList;

import java.util.List;

public interface SolrService {

    public SolrServer getSolrServer();

    public SolrServer getSolrServer(String url);

    public boolean addJsonDocumentToSolr(JSONObject document, String coreName, String hdfsKey);

    public boolean addDocumentToSolr(String content, String coreName, String hdfsKey);

    public boolean addJsonDocumentToSolrEmbeddedServer(JSONObject document, String coreName);

    public boolean deleteIndex(String coreName);

    public List<String> getCoreNames();

    public JSONObject getCoreData(String coreName);

    public JSONArray getAllCoreData();

    public String importCsvFileOnLocalSystemToSolr(String coreName, String fileName);

    public String importCsvFileOnServerToSolr(String coreName, String fileName);

}
