package service;

import net.sf.json.JSONObject;
import org.apache.nutch.parse.ParseData;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

public interface CoreService {

    public SolrServer getCloudSolrServer(String coreName);

    public SolrServer getSolrServer(String collectionName);

    public SolrServer getHttpSolrServer(String coreName);

    public int solrServerAdd(String coreName, List<SolrInputDocument> docs) throws IOException, SolrServerException;

    public int solrServerAdd(String coreName, SolrInputDocument doc) throws IOException, SolrServerException;

    public int update(String coreName, SolrInputDocument doc) throws IOException, SolrServerException;

    public int update(String coreName, List<SolrInputDocument> docs) throws IOException, SolrServerException;

    public int update(String coreName) throws IOException, SolrServerException;

    public int addInfoFiles(String coreName, HashMap<String, String> contents) throws IOException, SolrServerException;

    public SolrInputDocument createSolrDocument(HashMap<String, Object> fields);

    public SolrInputDocument createSolrInputDocumentFromNutch(String urlString, ParseData parseData, String segment,
                                                              String coreName, String contentType, String content);

    public int addNutchDocumentToSolr(String urlString, ParseData parseData, String segment,
                                          String coreName, String contentType, String content) throws IOException, SolrServerException;

    public int deleteIndex(String coreName) throws IOException, SolrServerException;

    public int deleteById(String coreName, List<String> ids) throws IOException, SolrServerException;

    public int deleteByField(String coreName, String field, String value) throws IOException, SolrServerException;

    public int deleteByField(String coreName, String field, List<String> values) throws IOException, SolrServerException;

    public JSONObject getCoreInfo(String coreName);

    public int doSolrOperation(String coreName, int operation, HashMap<String, String> params, StringWriter writer);
}
