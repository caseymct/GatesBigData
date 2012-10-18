package service;


import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class CoreServiceImpl implements CoreService {

    private SolrService solrService;

    @Autowired
    public void setServices(SolrService solrService) {
        this.solrService = solrService;
    }

    public SolrServer getSolrServer(String coreName) {
        try {
            return new CommonsHttpSolrServer(solrService.getSolrServerURI(coreName));

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public List<String> getFieldNamesFromLuke(String coreName) {
        List<String> fieldNames = new ArrayList<String>();
        String response = solrService.getSolrLukeData(coreName);

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

    public boolean isFieldMultiValued(SolrServer server, String fieldName) {
        LukeRequest luke = new LukeRequest();
        try {
            LukeResponse.FieldInfo fieldInfo = luke.process(server).getFieldInfo(fieldName);
            return fieldInfo != null && fieldInfo.getSchema().charAt(3) == 77;

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

    public boolean deleteIndex(String coreName) {

        try {
            SolrServer server = getSolrServer(coreName);
            server.deleteByQuery("*:*");
            solrService.solrServerCommit(server);

        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
            return false;

        } catch (IOException e) {
            System.out.println(e.getMessage());
            return false;
        }

        return true;
    }


    public boolean addDocumentToSolr(Object content, String hdfsKey, String coreName) {
        return solrService.addDocumentToSolr(content, hdfsKey, getSolrServer(coreName));
    }

    public JSONObject getCoreData(String coreName) {

        CoreAdminResponse cores = solrService.getCores();
        if (cores != null) {
            NamedList namedList = cores.getCoreStatus(coreName);
            if (namedList != null) {
                return JsonParsingUtils.constructJSONObjectFromNamedList(namedList);
            }
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
}
