package service;

import GatesBigData.utils.Constants;
import GatesBigData.utils.JsonParsingUtils;
import GatesBigData.utils.SolrUtils;
import net.sf.json.JSONArray;
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
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;


public class SolrServiceImpl implements SolrService {

    public static final Logger logger = Logger.getLogger(SolrServiceImpl.class);

    public SolrServer getSolrServer() {
        return getHttpSolrServer();
    }

    public SolrServer getCloudSolrServer() {
        try {
            CloudSolrServer solrServer = new CloudSolrServer(Constants.ZOOKEEPER_SERVER);
            solrServer.setZkClientTimeout(Constants.ZK_CLIENT_TIMEOUT);
            solrServer.setZkConnectTimeout(Constants.ZK_CONNECT_TIMEOUT);
            return solrServer;

        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public SolrServer getHttpSolrServer() {
        return new HttpSolrServer(SolrUtils.getSolrServerURI(null));
    }

    public int solrServerAdd(SolrServer server, SolrInputDocument doc) throws IOException, SolrServerException {
        return solrServerAdd(server, Arrays.asList(doc));
    }

    public int solrServerAdd(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException {
        return server.add(docs).getStatus();
    }

    public int solrServerAddAndCommit(SolrServer server, SolrInputDocument doc) throws IOException, SolrServerException {
        return solrServerAddAndCommit(server, Arrays.asList(doc));
    }

    public int solrServerAddAndCommit(SolrServer server, List<SolrInputDocument> docs) throws IOException, SolrServerException {
        if (Constants.SolrResponseSuccess(solrServerCommit(server))) {
            return solrServerUpdate(server, docs);
        }
        return Constants.SOLR_RESPONSE_CODE_ERROR;
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
        if (Constants.SolrResponseSuccess(rsp)) {
            return solrServerCommit(server);
        }
        return Constants.SOLR_RESPONSE_CODE_ERROR;
    }

    public int solrServerCommit(SolrServer server) throws IOException, SolrServerException {
        return server.commit().getStatus();
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
        return Constants.SolrResponseSuccess(server.deleteById(ids)) ? solrServerUpdate(server) : Constants.SOLR_RESPONSE_CODE_ERROR;
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
        Collections.sort(coreList);
        return coreList;
    }

    public boolean coreNameExists(String coreName) {
        return getCoreNames().contains(coreName);
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
