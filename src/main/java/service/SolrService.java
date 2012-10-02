package service;


import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.util.HashMap;
import java.util.List;

public interface SolrService {

    public String getSolrSchemaHDFSKey();

    public String getSolrSchemaHDFSSegment();

    public String getSolrServerURI();

    public String getSolrSuggestURI(String fieldSpecificEndpoint, HashMap<String,String> urlParams);

    public String getSolrSelectURI(HashMap<String,String> urlParams);

    public String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams);

    public String getSolrLukeURI(HashMap<String,String> urlParams);

    public String getSolrCoreURI(String coreName);

    public SolrServer getSolrServer();

    public SolrServer getSolrServer(String url);

    public List<String> getFieldNamesFromLuke();

    public boolean addJsonDocumentToSolr(JSONObject document, String coreName, String hdfsKey);

    public boolean addDocumentToSolr(String content, String coreName, String hdfsKey);

    public boolean deleteIndex(String coreName);

    public List<String> getCoreNames();

    public JSONObject getCoreData(String coreName);

    public Object getCoreDataIndexProperty(String coreName, String property);

    public JSONArray getAllCoreData();

    public String importCsvFileOnLocalSystemToSolr(String coreName, String fileName);

    public String importCsvFileOnServerToSolr(String coreName, String fileName);

    public HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs);

}
