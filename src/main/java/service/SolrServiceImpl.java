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
        String solrUrl = SolrUtils.getSolrServerURI(null);
        return new HttpSolrServer(solrUrl);
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
        CoreAdminResponse cores = getCores();
        if (cores != null) {
            for (int i = 0; i < cores.getCoreStatus().size(); i++) {
                if (coreName.equals(cores.getCoreStatus().getName(i))) {
                    return true;
                }
            }
        }
        return false;
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
