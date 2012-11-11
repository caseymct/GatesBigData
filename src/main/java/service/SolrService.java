package service;

import net.sf.json.JSONArray;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.List;

public interface SolrService {

    public SolrServer getSolrServer();

    public boolean solrServerCommit(SolrServer server, List<SolrInputDocument> docs);

    public boolean solrServerCommit(SolrServer server, SolrInputDocument doc);

    public boolean solrServerCommit(SolrServer server);

    public CoreAdminResponse getCores();

    public List<String> getCoreNames();

    public JSONArray getAllCoreData();

    public String importCsvFileOnLocalSystemToSolr(String coreName, String fileName);

    public String importCsvFileOnServerToSolr(String coreName, String fileName);
}
