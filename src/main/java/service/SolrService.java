package service;

import net.sf.json.JSONArray;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;

import java.util.List;

public interface SolrService {

    public SolrServer getSolrServer();

    public SolrServer getCloudSolrServer();

    public SolrServer getHttpSolrServer();

    public boolean solrServerCommit(SolrServer server, List<SolrInputDocument> docs);

    public boolean solrServerCommit(SolrServer server, SolrInputDocument doc);

    public boolean solrServerCommit(SolrServer server);

    public CoreAdminResponse getCores();

    public List<String> getCoreNames();

    public JSONArray getAllCoreData();
}
