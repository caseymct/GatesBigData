package service;

import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.SolrUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;


public class SolrServiceImpl implements SolrService {


    public SolrServer getSolrServer() {
        try {
            return new CommonsHttpSolrServer(SolrUtils.getSolrServerURI(null));

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public boolean solrServerCommit(SolrServer server, SolrInputDocument doc) {
        return solrServerCommit(server, Arrays.asList(doc));
    }

    public boolean solrServerCommit(SolrServer server, List<SolrInputDocument> docs) {
        try {
            server.commit();

            UpdateRequest req = new UpdateRequest();
            req.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);

            if (docs != null && docs.size() != 0) {
                req.add(docs);
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

    public boolean solrServerCommit(SolrServer server) {
        return solrServerCommit(server, new ArrayList<SolrInputDocument>());
    }

    public String importCsvFileOnServerToSolr(String coreName, String fileName) {
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("commit", "true");
        urlParams.put("f.categories.split", "true");
        urlParams.put("stream.file", fileName);
        urlParams.put("stream.contentType", "text/csv");

        String url = SolrUtils.getUpdateCsvEndpoint(coreName, urlParams);

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

        String url = SolrUtils.getUpdateCsvEndpoint(coreName, urlParams);
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

    public CoreAdminResponse getCores() {
        try {
            CoreAdminRequest request = new CoreAdminRequest();
            request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            return request.process(getSolrServer());

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public List<String> getCoreNames() {
        List<String> coreList = new ArrayList<String>();

        CoreAdminResponse cores = getCores();
        if (cores != null) {
            for (int i = 0; i < cores.getCoreStatus().size(); i++) {
                coreList.add(cores.getCoreStatus().getName(i));
            }
        }

        return coreList;
    }

    public JSONArray getAllCoreData() {
        JSONArray coreInfo = new JSONArray();

        CoreAdminResponse cores = getCores();
        if (cores != null) {
            for (Object o : cores.getCoreStatus()) {
                Map.Entry coreData = (Map.Entry) o;
                coreInfo.add(JsonParsingUtils.constructJSONObjectFromNamedList((NamedList) coreData.getValue()));
            }
        }

        return coreInfo;
    }
}
