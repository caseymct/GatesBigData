package service;

import net.sf.json.JSONArray;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.util.HashMap;
import java.util.List;

public interface SolrService {

    public String getSolrSchemaHDFSKey();

    public String getSolrSchemaHDFSSegment();

    public String getSolrServerURI(String coreName);

    public String getSolrSuggestURI(String fieldSpecificEndpoint, String coreName, HashMap<String,String> urlParams);

    public String getSolrSelectURI(String coreName, HashMap<String,String> urlParams);

    public String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams);

    public String getSolrLukeData();

    public String getSolrLukeData(String coreName);

    public String getSolrSchemaFieldName(String endpoint, boolean prefixField);

    public SolrServer getSolrServer();

    public boolean solrServerCommit(SolrServer server, SolrInputDocument doc);

    public boolean solrServerCommit(SolrServer server);

    public boolean addDocumentToSolr(Object content, String hdfsKey);

    public boolean addDocumentToSolr(Object content, String hdfsKey, SolrServer server);

    public CoreAdminResponse getCores();

    public List<String> getCoreNames();

    public JSONArray getAllCoreData();

    public String importCsvFileOnLocalSystemToSolr(String coreName, String fileName);

    public String importCsvFileOnServerToSolr(String coreName, String fileName);

    public HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs);

}
