package service;

import GatesBigData.utils.*;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;

import org.apache.solr.schema.DateField;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.*;

public class CoreServiceImpl implements CoreService {

    private SolrService solrService;



    private NtlmPasswordAuthentication auth;
    private static final Logger logger = Logger.getLogger(CoreServiceImpl.class);

    HashMap<String, String> mapMetadataToSolrFields = new HashMap<String, String>() {{
        put("Last-Save-Date",   Constants.SOLR_FIELD_NAME_LAST_MODIFIED);
        put("Creation-Date",    Constants.SOLR_FIELD_NAME_CREATE_DATE);
        put("Last-Author",      Constants.SOLR_FIELD_NAME_LAST_AUTHOR);
        put("Application-Name", Constants.SOLR_FIELD_NAME_APPLICATION_NAME);
        put("Author",           Constants.SOLR_FIELD_NAME_AUTHOR);
        put("Company",          Constants.SOLR_FIELD_NAME_COMPANY);
        put("title",            Constants.SOLR_FIELD_NAME_TITLE);
    }};

    @Autowired
    public void setServices(SolrService solrService) {
        this.solrService = solrService;
    }

    private NtlmPasswordAuthentication getAuth() {
        if (this.auth == null) {
            this.auth = new NtlmPasswordAuthentication(Constants.SMB_DOMAIN, Constants.SMB_USERNAME, Constants.SMB_PASSWORD);
        }
        return this.auth;
    }

    public SolrServer getCloudSolrServer(String collectionName) {
        try {
            CloudSolrServer solrServer = new CloudSolrServer(Constants.ZOOKEEPER_SERVER);
            solrServer.setDefaultCollection(collectionName);
            solrServer.setZkClientTimeout(Constants.ZK_CLIENT_TIMEOUT);
            solrServer.setZkConnectTimeout(Constants.ZK_CONNECT_TIMEOUT);
            return solrServer;
        } catch (MalformedURLException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public SolrServer getHttpSolrServer(String coreName) {
        return new HttpSolrServer(SolrUtils.getSolrServerURI(coreName));
    }

    public SolrServer getSolrServer(String collectionName) {
        return getHttpSolrServer(collectionName);
    }

    public int solrServerAdd(String coreName, List<SolrInputDocument> docs) throws IOException, SolrServerException {
        return solrService.solrServerAdd(getSolrServer(coreName), docs);
    }

    public int solrServerAdd(String coreName, SolrInputDocument doc) throws IOException, SolrServerException {
        return solrService.solrServerAdd(getSolrServer(coreName), doc);
    }

    public int deleteIndex(String coreName) throws IOException, SolrServerException{
        return solrService.solrServerDeleteIndex(getSolrServer(coreName));
    }

    public int deleteById(String coreName, List<String> ids) throws IOException, SolrServerException{
        return solrService.solrServerDeleteById(getSolrServer(coreName), ids);
    }

    public int deleteByField(String coreName, String field, String value) throws IOException, SolrServerException{
        return deleteByField(coreName, field, Arrays.asList(value));
    }

    public int deleteByField(String coreName, String field, List<String> values) throws IOException, SolrServerException {
        return solrService.solrServerDeleteByField(getSolrServer(coreName), field, values);
    }

    public int update(String coreName, SolrInputDocument doc) throws IOException, SolrServerException {
        return solrService.solrServerUpdate(getSolrServer(coreName), Arrays.asList(doc));
    }

    public int update(String coreName, List<SolrInputDocument> docs) throws IOException, SolrServerException {
        return solrService.solrServerUpdate(getSolrServer(coreName), docs);
    }

    public int update(String coreName) throws IOException, SolrServerException {
        return solrService.solrServerUpdate(getSolrServer(coreName));
    }

    public SolrInputDocument createSolrDocument(HashMap<String, Object> fields) {
        SolrInputDocument doc = new SolrInputDocument();

        for(Map.Entry<String, Object> entry : fields.entrySet()) {
            doc.addField(entry.getKey(), entry.getValue());
        }

        return doc;
    }

    public int addInfoFiles(String coreName, HashMap<String, String> hdfsInfoFileContents) throws IOException, SolrServerException {
        List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        List<String> fieldsToDelete = new ArrayList<String>();

        for(Map.Entry<String, String> entry : hdfsInfoFileContents.entrySet()) {
            final String title   = entry.getKey();
            final String content = entry.getValue();
            fieldsToDelete.add(title);

            HashMap<String, Object> params = new HashMap<String, Object>() {{
                put(title, content);
                put(Constants.SOLR_FIELD_NAME_TITLE, title);
                put(Constants.SOLR_FIELD_NAME_ID, UUID.nameUUIDFromBytes(title.getBytes()));
                put(Constants.SOLR_FIELD_NAME_URL, title);
            }};
            docs.add(createSolrDocument(params));
        }

        deleteByField(coreName, Constants.SOLR_FIELD_NAME_TITLE, fieldsToDelete);
        return update(coreName, docs);
    }


    private HashMap<String, Object> addParseMeta(HashMap<String, Object> params, Metadata metadata) {
        List<String> names = Arrays.asList(metadata.names());
        for (Map.Entry<String,String> entry : mapMetadataToSolrFields.entrySet()) {
            if (names.contains(entry.getKey())) {
                params.put(entry.getValue(), metadata.get(entry.getKey()));
            }
        }
        return params;
    }

    private HashMap<String, Object> addSMBDates(HashMap<String, Object> params, String url) {
        boolean addLastModified = !params.containsKey(Constants.SOLR_FIELD_NAME_LAST_MODIFIED);
        boolean addCreateTime   = !params.containsKey(Constants.SOLR_FIELD_NAME_CREATE_DATE);

        if (addCreateTime || addLastModified) {
            try {
                SmbFile file = new SmbFile(url, getAuth());
                if (file.exists()) {
                    if (addLastModified) params.put(Constants.SOLR_FIELD_NAME_LAST_MODIFIED, DateField.formatExternal(new Date(file.getLastModified())));
                    if (addCreateTime)   params.put(Constants.SOLR_FIELD_NAME_CREATE_DATE, DateField.formatExternal(new Date(file.createTime())));
                }
            } catch (SmbException e) {
                logger.error(e.getMessage());
            } catch (MalformedURLException e){
                logger.error(e.getMessage());
            }
        }
        return params;
    }

    public SolrInputDocument createSolrInputDocumentFromNutch(final String urlString, ParseData parseData, final String segment, String coreName,
                                                              final String contentType, String content) {
        if (parseData == null || !parseData.getStatus().isSuccess()) {
            return null;
        }
        HashMap<String, Object> params = new HashMap<String, Object>() {{
            put(Constants.SOLR_FIELD_NAME_HDFSKEY, urlString);
            put(Constants.SOLR_FIELD_NAME_HDFSSEGMENT, segment);
            put(Constants.SOLR_FIELD_NAME_BOOST, 1.0);
            put(Constants.SOLR_FIELD_NAME_ID, urlString);
            put(Constants.SOLR_FIELD_NAME_URL, urlString);
            put(Constants.SOLR_FIELD_NAME_TIMESTAMP, DateField.formatExternal(Calendar.getInstance().getTime()));
            put(Constants.SOLR_FIELD_NAME_CONTENT_TYPE, contentType);
        }};
        params = addParseMeta(params, parseData.getParseMeta());

        if (urlString.startsWith(Constants.SMB_PROTOCOL)) {
            params = addSMBDates(params, urlString);
        }

        if (!params.containsKey(Constants.SOLR_FIELD_NAME_TITLE)) {
            String[] urlItems = urlString.split("/");
            params.put(Constants.SOLR_FIELD_NAME_TITLE, urlItems[urlItems.length - 1]);
        }

        if (contentType.equals(Constants.JSON_CONTENT_TYPE)) {
            params = addFieldsFromJsonObject(params, content, "");
        } else if (!Utils.nullOrEmpty(content)) {
            params.put(Constants.SOLR_FIELD_NAME_CONTENT, content);
        }

        return createSolrDocument(params);
    }

    public int addNutchDocumentToSolr(String urlString, ParseData parseData, String segment,
                                      String coreName, String contentType, String content) throws IOException, SolrServerException {
        SolrInputDocument doc = createSolrInputDocumentFromNutch(urlString, parseData, segment, coreName, contentType, content);
        return update(coreName, doc);
    }

    private HashMap<String, Object> addFieldsFromJsonObject(HashMap<String, Object> params, JSONObject jsonObject, String parentStr) {
        for(Object key : jsonObject.keySet()) {
            if (jsonObject.get(key) instanceof JSONObject) {
                params = addFieldsFromJsonObject(params, jsonObject.getJSONObject(key.toString()), key + ".");
            } else {
                params.put(parentStr + key, jsonObject.get(key));
            }
        }
        return params;
    }

    private HashMap<String, Object> addFieldsFromJsonObject(HashMap<String, Object> params, String jsonObjectStr, String parentStr) {
        try {
            JSONObject jsonObject = JSONObject.fromObject(jsonObjectStr);
            return addFieldsFromJsonObject(params, jsonObject, parentStr);
        } catch (JSONException e) {
            logger.error("Invalid json");
        }
        return params;
    }

    public JSONObject getCoreInfo(String coreName) {
        CoreAdminResponse coreAdminResponse = solrService.getCores();
        return JsonParsingUtils.constructJSONObjectFromNamedList(coreAdminResponse.getCoreStatus(coreName));
    }

    public int doSolrOperation(String coreName, int operation, HashMap<String, String> params, StringWriter writer) {
        int success = Constants.SOLR_RESPONSE_CODE_ERROR;
        String msg = Constants.SOLR_OPERATION_MSGS.get(operation);

        writer.write("Performing operation " + msg.toLowerCase() + " on core " + coreName + ".\n");

        try {
            if (operation == Constants.SOLR_OPERATION_UPDATE) {
                success = update(coreName);
            } else if (operation == Constants.SOLR_OPERATION_DELETE) {
                success = deleteIndex(coreName);
            } else if (operation == Constants.SOLR_OPERATION_ADD_INFOFILES) {
                success = addInfoFiles(coreName, params);
            } else {
                writer.write("Invalid operation code: " + operation + "\n");
            }

            writer.write("\t" + msg + " success: " + Constants.SolrResponseSuccess(success) + "\n");
        } catch (SolrServerException e) {
            writer.write("\t" + msg + " failed with SolrServerException: " + e.getMessage() + "\n");
        } catch (IOException e) {
            writer.write("\t" + msg + " failed with IOException: " + e.getMessage() + "\n");
        }

        writer.write("\n");
        return success;
    }
}
