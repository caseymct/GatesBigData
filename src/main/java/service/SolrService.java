package service;

import net.sf.json.JSONArray;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

public interface SolrService {

    public SolrServer getSolrServer();

    public SolrServer getCloudSolrServer();

    public SolrServer getHttpSolrServer();

    public int solrServerAdd(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException;

    public int solrServerAdd(SolrServer server, SolrInputDocument doc) throws IOException, SolrServerException;

    public int solrServerCommit(SolrServer server) throws IOException, SolrServerException;

    public int solrServerUpdate(SolrServer server) throws IOException, SolrServerException;

    public int solrServerUpdate(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException;

    public int solrServerAddAndCommit(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException;

    public int solrServerAddAndCommit(SolrServer server, SolrInputDocument doc) throws IOException, SolrServerException;

    public int solrServerDeleteByField(SolrServer server, String field, List<String> values) throws IOException, SolrServerException;

    public int solrServerDeleteByField(SolrServer server, String field, String value) throws IOException, SolrServerException;

    public int solrServerDeleteIndex(SolrServer server) throws IOException, SolrServerException;

    public int solrServerDeleteById(SolrServer server, List<String> ids) throws IOException, SolrServerException;

    public CoreAdminResponse getCores();

    public List<String> getCoreNames();

    public boolean coreNameExists(String coreName);

    public JSONArray getAllCoreData();
}
