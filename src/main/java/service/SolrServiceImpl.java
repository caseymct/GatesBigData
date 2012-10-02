package service;

import LucidWorksApp.utils.FieldUtils;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;


public class SolrServiceImpl implements SolrService {

    static final String SOLR_SCHEMA_HDFSKEY     = "HDFSKey";
    static final String SOLR_SCHEMA_HDFSSEGMENT = "HDFSSegment";

    public static final String SOLR_SERVER = "http://denlx006.dn.gates.com:8983";
    //public static final String SOLR_SERVER = "http://localhost:8983";
    public static final String SOLR_ENDPOINT = "solr";
    public static final String SOLR_SUGGEST_ENDPOINT = "suggest";
    public static final String SOLR_SELECT_ENDPOINT = "select";

    public static final String UPDATECSV_ENDPOINT = "update/csv";
    public static final String LUKE_ENDPOINT = "admin/luke";

    public String getSolrSchemaHDFSKey() {
        return SOLR_SCHEMA_HDFSKEY;
    }

    public String getSolrSchemaHDFSSegment() {
        return SOLR_SCHEMA_HDFSSEGMENT;
    }

    public String getSolrServerURI() {
        return SOLR_SERVER + "/" + SOLR_ENDPOINT;
    }

    public String getSolrSuggestURI(String fieldSpecificEndpoint, HashMap<String,String> urlParams) {
        String uri = getSolrServerURI() + "/" + SOLR_SUGGEST_ENDPOINT;
        if (fieldSpecificEndpoint != null && !fieldSpecificEndpoint.equals("")) {
            uri += "/" + fieldSpecificEndpoint;
        }
        return uri + Utils.constructUrlParams(urlParams);
    }

    public String getSolrSelectURI(HashMap<String,String> urlParams) {
        return getSolrServerURI() + "/" + SOLR_SELECT_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams) {
        return getSolrServerURI() + "/" + coreName + "/" + UPDATECSV_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public String getSolrLukeURI(HashMap<String,String> urlParams) {
        return getSolrServerURI() + "/" + LUKE_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public String getSolrCoreURI(String coreName) {
        return getSolrServerURI() + "/" + coreName;
    }

    public SolrServer getSolrServer() {
        try {
            return new CommonsHttpSolrServer(getSolrServerURI());

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public SolrServer getSolrServer(String url) {
        try {
            return new CommonsHttpSolrServer(url);

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private boolean solrServerCommit(SolrServer server, SolrInputDocument doc) {
        try {
            server.commit();

            UpdateRequest req = new UpdateRequest();
            req.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);

            if (doc != null) {
                req.add(doc);
            }
            UpdateResponse rsp = req.process(server);
            rsp.getStatus();

        } catch (SolrServerException e) {
            System.err.println(e.getMessage());
            return false;

        } catch (IOException e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    private boolean solrServerCommit(SolrServer server) {
        return solrServerCommit(server, null);
    }

    public List<String> getFieldNamesFromLuke() {
        List<String> fieldNames = new ArrayList<String>();

        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("numTerms", "0");
        urlParams.put("wt", "json");

        String response = HttpClientUtils.httpGetRequest(getSolrLukeURI(urlParams));

        try {
            JSONObject json = JSONObject.fromObject(response);
            JSONObject fields = (JSONObject) json.get("fields");

            List<Object> fieldNameObjects = Arrays.asList(fields.names().toArray());
            for(Object f : fieldNameObjects) {
                fieldNames.add((String) f);
            }

        } catch (JSONException e) {
            System.out.println(e.getMessage());
        }
        return fieldNames;
    }

    public boolean addJsonDocumentToSolr(JSONObject document, String coreName, String hdfsKey) {

        SolrInputDocument doc = new SolrInputDocument();

        doc.addField(SOLR_SCHEMA_HDFSKEY, hdfsKey);

        for(Object key : document.keySet()) {
            if (document.get(key) instanceof JSONObject) {
                JSONObject subDocument = (JSONObject) document.get(key);
                for (Object subKey : subDocument.keySet()) {
                    doc.addField((String) key + "." + (String) subKey, subDocument.get(subKey));
                }
            } else {
                doc.addField((String) key, document.get(key), 1.0f);
            }
        }

        return solrServerCommit(getSolrServer(), doc);
    }

    public boolean addDocumentToSolr(String content, String coreName, String hdfsKey) {

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SOLR_SCHEMA_HDFSKEY, hdfsKey);
        doc.addField("content", content);

        return solrServerCommit(getSolrServer(), doc);
    }

    public boolean deleteIndex(String coreName) {

        try {
            SolrServer server = getSolrServer();
            server.deleteByQuery("*:*");
            solrServerCommit(server);

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
            return false;

        } catch (IOException e) {
            System.out.println(e.getMessage());
            return false;
        }

        return true;
    }

    public String importCsvFileOnServerToSolr(String coreName, String fileName) {
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("commit", "true");
        urlParams.put("f.categories.split", "true");
        urlParams.put("stream.file", fileName);
        urlParams.put("stream.contentType", "text/csv");

        String url = getUpdateCsvEndpoint(coreName, urlParams);

        String response = HttpClientUtils.httpGetRequest(url);
        if (!response.contains("Errors")) {
           // FieldUtils.updateCSVFilesUploadedField(coreName, fileName, true);
        }
        return response;
    }

    public String importCsvFileOnLocalSystemToSolr(String coreName, String fileName) {
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("commit", "true");
        urlParams.put("f.categories.split", "true");

        String url = getUpdateCsvEndpoint(coreName, urlParams);
        //String urlParams = "?commit=true&f.categories.split=true";

        //curl http://localhost:8983/solr/update/csv --data-binary @books.csv -H 'Content-type:text/plain; charset=utf-8'

        // SolrServer server = new CommonsHttpSolrServer(SOLR_SERVER + SOLR_ENDPOINT);
        // ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/csv");
        // req.addFile(new File(filename));
        // req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        // NamedList result = server.request(req);
        // System.out.println("Result: " + result);

        String response = HttpClientUtils.httpBinaryDataPostRequest(url, fileName);
        if (!response.contains("Errors")) {
        //    FieldUtils.updateCSVFilesUploadedField(coreName, fileName, false);
        }
        return response;
    }

    public List<String> getCoreNames() {
        List<String> coreList = new ArrayList<String>();

        try {
            CoreAdminRequest request = new CoreAdminRequest();
            request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            CoreAdminResponse cores = request.process(getSolrServer());

            for (int i = 0; i < cores.getCoreStatus().size(); i++) {
                coreList.add(cores.getCoreStatus().getName(i));
            }

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return coreList;
    }

    public JSONObject getCoreData(String coreName) {

        try {
            CoreAdminRequest request = new CoreAdminRequest();
            request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            CoreAdminResponse cores = request.process(getSolrServer());

            return JsonParsingUtils.constructJSONObjectFromNamedList(cores.getCoreStatus(coreName));

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return new JSONObject();
    }

    public Object getCoreDataIndexProperty(String coreName, String property) {
        JSONObject coreData = getCoreData(coreName);
        if (coreData.has("index")) {
            JSONObject index = (JSONObject) coreData.get("index");
            if (index.has(property)) {
                return index.get(property);
            }
        }
        return null;
    }

    public JSONArray getAllCoreData() {
        JSONArray coreInfo = new JSONArray();

        try {
            CoreAdminRequest request = new CoreAdminRequest();
            request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            CoreAdminResponse cores = request.process(getSolrServer());

            for (Object o : cores.getCoreStatus()) {
                Map.Entry coreData = (Map.Entry) o;
                coreInfo.add(JsonParsingUtils.constructJSONObjectFromNamedList((NamedList) coreData.getValue()));
            }
            return coreInfo;

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return new JSONArray();
    }

    public HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for (SolrDocument doc : docs) {
            String hdfsSeg = (String) doc.getFieldValue(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add((String) doc.getFieldValue(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }
}
