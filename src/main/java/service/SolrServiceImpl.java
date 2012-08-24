package service;

import LucidWorksApp.utils.FieldUtils;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;


public class SolrServiceImpl implements SolrService {

    public SolrServer getSolrServer() {
        try {
            String url = Utils.getServer() + Utils.getSolrEndpoint();
            return new CommonsHttpSolrServer(url);

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
            req.setAction(UpdateRequest.ACTION.COMMIT, false, false );

            if (doc != null) {
                req.add(doc);
            }
            UpdateResponse rsp = req.process(server);

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

    public boolean addJsonDocumentToSolrEmbeddedServer(JSONObject document, String coreName) {
        String solrHome = Utils.getSolrPath();

        try {
            File home = new File(solrHome);
            File f = new File(home, "solr.xml");
            CoreContainer container = new CoreContainer();
            container.load(solrHome, f);

            EmbeddedSolrServer server = new EmbeddedSolrServer(container, coreName);

            SolrInputDocument doc = new SolrInputDocument();
            for(Object key : document.keySet()) {
                doc.addField((String) key, document.get(key), 1.0f);
            }
            server.add(doc);
            boolean added = solrServerCommit(server, doc);
            container.shutdown();
            return added;

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        } catch (ParserConfigurationException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (SAXException e) {
            System.out.println(e.getMessage());
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        }

        return false;
    }

    public boolean addJsonDocumentToSolr(JSONObject document, String coreName, String hdfsKey) {

        SolrInputDocument doc = new SolrInputDocument();

        doc.addField(Utils.getSolrSchemaHdfskey(), hdfsKey);
        for(Object key : document.keySet()) {
            doc.addField((String) key, document.get(key), 1.0f);
        }

        return solrServerCommit(getSolrServer(), doc);
    }

    public boolean deleteIndex(String coreName) {

        try {
            SolrServer server = getSolrServer();
            server.deleteByQuery( "*:*" );
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

    public String importCsvFileOnServerToSolr(String collectionName, String fileName) {
        /*
        String url = SERVER + SOLR_ENDPOINT + UPDATECSV_ENDPOINT;
        String urlParams = "?stream.file=" + fileName + "&stream.contentType=text/plain;charset=utf-8";
        straight solr
        */
        String url = Utils.getServer() + Utils.getSolrEndpoint() + "/" + collectionName + Utils.getUpdateCsvEndpoint();
        String urlParams = "?commit=true&f.categories.split=true&stream.file=" +
                fileName + "&stream.contentType=text/csv";

        String response = HttpClientUtils.httpGetRequest(url + urlParams);
        if (!response.contains("Errors")) {
            FieldUtils.updateCSVFilesUploadedField(collectionName, fileName, true);
        }
        return response;
    }

    public String importCsvFileOnLocalSystemToSolr(String collectionName, String fileName) {
        String url = Utils.getServer() + Utils.getSolrEndpoint() + "/" + collectionName + Utils.getUpdateCsvEndpoint();
        String urlParams = "?commit=true&f.categories.split=true";

        //curl http://localhost:8983/solr/update/csv --data-binary @books.csv -H 'Content-type:text/plain; charset=utf-8'

        // SolrServer server = new CommonsHttpSolrServer(SERVER + SOLR_ENDPOINT);
        // ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/csv");
        // req.addFile(new File(filename));
        // req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        // NamedList result = server.request(req);
        // System.out.println("Result: " + result);

        String response = HttpClientUtils.httpBinaryDataPostRequest(url + urlParams, fileName);
        if (!response.contains("Errors")) {
            FieldUtils.updateCSVFilesUploadedField(collectionName, fileName, false);
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
}
