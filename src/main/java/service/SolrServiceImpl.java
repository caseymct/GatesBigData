package service;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.Response;
import GatesBigData.constants.solr.Solr;
import GatesBigData.utils.*;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;


public class SolrServiceImpl implements SolrService {

    public static final Logger logger = Logger.getLogger(SolrServiceImpl.class);

    public SolrServer getSolrServer() {
        return getSolrServer(null);
    }

    public SolrServer getSolrServer(String collectionName) {
        return Solr.USE_CLOUD_SOLR_SERVER ? getCloudSolrServer(collectionName) : getHttpSolrServer(collectionName);
    }

    public SolrServer getCloudSolrServer(String collectionName) {
        try {
            CloudSolrServer solrServer = new CloudSolrServer(Solr.ZOOKEEPER_SERVER);
            solrServer.setZkClientTimeout(Solr.ZK_CLIENT_TIMEOUT);
            solrServer.setZkConnectTimeout(Solr.ZK_CONNECT_TIMEOUT);

            if (!Utils.nullOrEmpty(collectionName)) solrServer.setDefaultCollection(collectionName);

            return solrServer;

        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public SolrServer getHttpSolrServer(String coreName) {
        return new HttpSolrServer(SolrUtils.getSolrServerURI(coreName));
    }

    public SolrServer getHttpSolrServer() {
        return getHttpSolrServer(null);
    }

    public List<SolrServer> getCloudSolrServers() {
        List<SolrServer> servers = new ArrayList<SolrServer>();
        for(String cloudSolrServer : Solr.CLOUD_SOLR_SERVERS) {
            servers.add(new HttpSolrServer(cloudSolrServer));
        }
        return servers;
    }

    public int solrServerAdd(SolrServer server, SolrInputDocument doc) throws IOException, SolrServerException {
        return solrServerAdd(server, Arrays.asList(doc));
    }

    public int solrServerAdd(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException {
        return server.add(docs).getStatus();
    }

    public int solrServerUpdate(SolrServer server) throws IOException, SolrServerException {
        return solrServerUpdate(server, null);
    }

    public int solrServerUpdate(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException {

        UpdateRequest req = new UpdateRequest();
        req.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);

        if (docs != null && docs.size() != 0) {
            req.add(docs);
        }

        UpdateResponse rsp = req.process(server);
        if (Response.success(rsp)) {
            return solrServerCommit(server);
        }
        return Response.CODE_ERROR;
    }

    public int solrServerCommit(SolrServer server) throws IOException, SolrServerException {
        return server.commit().getStatus();
    }

    public int solrServerOptimize(SolrServer server) throws IOException, SolrServerException {
        return server.optimize().getStatus();
    }

    public int solrServerDeleteIndex(SolrServer server) throws IOException, SolrServerException {
        return solrServerDeleteByField(server, "*", Arrays.asList("*"));
    }

    public int solrServerDeleteByField(SolrServer server, String field, String value) throws IOException, SolrServerException {
        return solrServerDeleteByField(server, field, Arrays.asList(value));
    }

    public int solrServerDeleteByField(SolrServer server, String field, List<String> values) throws IOException, SolrServerException {
        for(String value : values) {
            server.deleteByQuery(field + ":" + value);
        }
        return solrServerUpdate(server);
    }

    public int solrServerDeleteById(SolrServer server, List<String> ids) throws IOException, SolrServerException {
        return Response.success(server.deleteById(ids)) ? solrServerUpdate(server) : Response.CODE_ERROR;
    }

    public CoreAdminResponse getCores() {
        try {
            CoreAdminRequest request = new CoreAdminRequest();
            request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            return request.process(getHttpSolrServer());

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public JSONObject getCollectionInfo() {
        String clusterStateStr = HttpClientUtils.httpGetRequest(SolrUtils.getSolrClusterStateURI());
        JSONObject clusterStateJson = JSONUtils.convertStringToJSONObject(clusterStateStr);

        return (JSONObject) JSONUtils.extractJSONProperty(clusterStateJson, Arrays.asList("znode", "data"),
                JSONObject.class, null);
    }

    public List<String> getCollectionNames() {
        JSONObject clusterState = getCollectionInfo();
        if (Utils.nullOrEmpty(clusterState)) {
            return new ArrayList<String>();
        }

        return JSONUtils.convertJSONArrayToStringList(clusterState.names());
    }
}
