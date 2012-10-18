package service;

import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;


public class SolrServiceImpl implements SolrService {

    static final String SOLR_SCHEMA_HDFSKEY     = "HDFSKey";
    static final String SOLR_SCHEMA_HDFSSEGMENT = "HDFSSegment";

    static final String SOLR_SCHEMA_USER_FULL_FIELD        = "User.UserName";
    static final String SOLR_SCHEMA_SUPPLIER_FULL_FIELD    = "Supplier.SupplierName";
    static final String SOLR_SCHEMA_COMPANYSITE_FULL_FIELD = "CompanySite.SiteName";
    static final String SOLR_SCHEMA_ACCOUNT_FULL_FIELD     = "Account.AccountName";
    static final String SOLR_SCHEMA_COSTCENTER_FULL_FIELD  = "CostCenter.CostCenterName";

    static final String SOLR_SCHEMA_USER_PREFIX_FIELD        = "UserNamePrefix";
    static final String SOLR_SCHEMA_SUPPLIER_PREFIX_FIELD    = "SupplierNamePrefix";
    static final String SOLR_SCHEMA_COMPANYSITE_PREFIX_FIELD = "CompanySiteNamePrefix";
    static final String SOLR_SCHEMA_ACCOUNT_PREFIX_FIELD     = "AccountNamePrefix";
    static final String SOLR_SCHEMA_COSTCENTER_PREFIX_FIELD  = "CostCenterNamePrefix";

    static final int SOLR_SUGGEST_USER_INDEX          = 0;
    static final int SOLR_SUGGEST_SUPPLIER_INDEX      = 1;
    static final int SOLR_SUGGEST_ACCOUNT_INDEX       = 2;
    static final int SOLR_SUGGEST_COMPANYSITE_INDEX   = 3;
    static final int SOLR_SUGGEST_COSTCENTER_INDEX    = 4;
    static final List<String> SOLR_SUGGEST_ENDPOINT_LIST = new ArrayList<String>(
            Arrays.asList("user", "supplier", "account", "companysite", "costcenter"));

    public static final String SOLR_SERVER = "http://denlx006.dn.gates.com:8984";
    //public static final String SOLR_SERVER = "http://localhost:8983";
    public static final String SOLR_ENDPOINT         = "solr";
    public static final String SOLR_SUGGEST_ENDPOINT = "suggest";
    public static final String SOLR_SELECT_ENDPOINT  = "select";
    public static final String UPDATECSV_ENDPOINT    = "update/csv";
    public static final String LUKE_ENDPOINT         = "admin/luke";

    public String getSolrSchemaHDFSKey() {
        return SOLR_SCHEMA_HDFSKEY;
    }

    public String getSolrSchemaHDFSSegment() {
        return SOLR_SCHEMA_HDFSSEGMENT;
    }

    public String getSolrServerURI(String coreName) {
        return Utils.addToUrlIfNotEmpty(SOLR_SERVER + "/" + SOLR_ENDPOINT, coreName);
    }

    public String getSolrSuggestURI(String fieldSpecificEndpoint, String coreName, HashMap<String,String> urlParams) {
        String uri = getSolrServerURI(coreName) + "/" + SOLR_SUGGEST_ENDPOINT;
        return Utils.addToUrlIfNotEmpty(uri, fieldSpecificEndpoint) + Utils.constructUrlParams(urlParams);
    }

    public String getSolrSelectURI(String coreName, HashMap<String,String> urlParams) {
        return getSolrServerURI(coreName) + "/" + SOLR_SELECT_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams) {
        return getSolrServerURI(coreName) + "/" + UPDATECSV_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public String getSolrLukeData() {
        return getSolrLukeData(null);
    }

    public String getSolrLukeData(String coreName) {
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("numTerms", "0");
        urlParams.put("wt", "json");
        String url = getSolrServerURI(coreName) + "/" + LUKE_ENDPOINT + Utils.constructUrlParams(urlParams);

        return HttpClientUtils.httpGetRequest(url);
    }

    private int getSolrSuggestEndpointIndexFromString(String endpoint) {
        return SOLR_SUGGEST_ENDPOINT_LIST.indexOf(endpoint);
    }

    public String getSolrSchemaFieldName(String endpoint, boolean prefixField) {
        switch (getSolrSuggestEndpointIndexFromString(endpoint)) {
            case SOLR_SUGGEST_ACCOUNT_INDEX:
                return prefixField ? SOLR_SCHEMA_ACCOUNT_PREFIX_FIELD : SOLR_SCHEMA_ACCOUNT_FULL_FIELD;
            case SOLR_SUGGEST_COMPANYSITE_INDEX:
                return prefixField ? SOLR_SCHEMA_COMPANYSITE_PREFIX_FIELD : SOLR_SCHEMA_COMPANYSITE_FULL_FIELD;
            case SOLR_SUGGEST_COSTCENTER_INDEX:
                return prefixField ? SOLR_SCHEMA_COSTCENTER_PREFIX_FIELD : SOLR_SCHEMA_COSTCENTER_FULL_FIELD;
            case SOLR_SUGGEST_SUPPLIER_INDEX:
                return prefixField ? SOLR_SCHEMA_SUPPLIER_PREFIX_FIELD : SOLR_SCHEMA_SUPPLIER_FULL_FIELD;
            case SOLR_SUGGEST_USER_INDEX:
                return prefixField ? SOLR_SCHEMA_USER_PREFIX_FIELD : SOLR_SCHEMA_USER_FULL_FIELD;
            default:
                return null;
        }
    }

    public SolrServer getSolrServer() {
        try {
            return new CommonsHttpSolrServer(getSolrServerURI(null));

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public boolean solrServerCommit(SolrServer server, SolrInputDocument doc) {
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

    public boolean solrServerCommit(SolrServer server) {
        return solrServerCommit(server, null);
    }

    public boolean addDocumentToSolr(Object content, String hdfsKey) {
        return addDocumentToSolr(content, hdfsKey, getSolrServer());
    }

    public boolean addDocumentToSolr(Object content, String hdfsKey, SolrServer server) {

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SOLR_SCHEMA_HDFSKEY, hdfsKey);

        if (content instanceof JSONObject) {
            JSONObject document = (JSONObject) content;
            for(Object key : document.keySet()) {
                if (document.get(key) instanceof JSONObject) {
                    JSONObject subDocument = (JSONObject) document.get(key);
                    for (Object subKey : subDocument.keySet()) {
                        doc.addField(key + "." + subKey, subDocument.get(subKey));
                    }
                } else {
                    doc.addField((String) key, document.get(key), 1.0f);
                }
            }
        } else {
            doc.addField("content", content);
        }

        return solrServerCommit(server, doc);
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
