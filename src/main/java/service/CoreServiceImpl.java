package service;


import LucidWorksApp.utils.DateUtils;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
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
import org.apache.solr.schema.DateField;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
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

    public boolean fieldExists(SolrServer server, String fieldName) {
        LukeRequest luke = new LukeRequest();
        try {
            LukeResponse.FieldInfo fieldInfo = luke.process(server).getFieldInfo(fieldName);
            return fieldInfo != null;

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

    private String parseDateFromSolrResponse(String url, String field) {
        String response = HttpClientUtils.httpGetRequest(url);
        JSONObject jsonObject = JSONObject.fromObject(response);
        String date = jsonObject.getJSONObject("response").getJSONArray("docs").getJSONObject(0).getString(field);
        return DateUtils.getSolrDate(date);
    }

    public List<String> getSolrFieldDateRange(String coreName, String field, String format) {
        List<String> dateRange = new ArrayList<String>();

        int buckets = 10;
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("q", field + ":*");
        urlParams.put("rows", "1");
        urlParams.put("wt", "json");
        urlParams.put("fl", field);
        urlParams.put("sort", field + "+asc");

        String date = parseDateFromSolrResponse(solrService.getSolrSelectURI(coreName, urlParams), field);
        dateRange.add(DateUtils.getFormattedDateStringFromSolrDate(date, format));

        urlParams.put("sort", field + "+desc");
        date = parseDateFromSolrResponse(solrService.getSolrSelectURI(coreName, urlParams), field);
        dateRange.add(DateUtils.getFormattedDateStringFromSolrDate(date, format));
        /*url = solrService.getSolrSelectURI(coreName, urlParams);
        jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url));
        date = jsonObject.getJSONObject("response").getJSONArray("docs").getJSONObject(0).getString(field);
        dateRange.add(DateUtils.getFormattedDateStringFromSolrDate(DateUtils.getSolrDate(date), format));   */

        if (format.equals(DateUtils.SOLR_DATE)) {
            try {
                Long ms = (DateField.parseDate(dateRange.get(1)).getTime() - DateField.parseDate(dateRange.get(0)).getTime())/buckets;
                dateRange.add(DateUtils.getDateGapString(ms));
            } catch (ParseException e) {
                System.err.println(e.getCause());
            }
        }
        return dateRange;
    }
}
