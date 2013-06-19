package service;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.Operations;
import GatesBigData.constants.solr.Response;
import GatesBigData.constants.solr.Solr;
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

    public SolrServer getSolrServer(String collectionName) {
        return solrService.getSolrServer(collectionName);
    }

    public int solrServerAdd(String coreName, List<SolrInputDocument> docs) throws IOException, SolrServerException {
        return solrService.solrServerAdd(getSolrServer(coreName), docs);
    }

    public int solrServerAdd(String coreName, SolrInputDocument doc) throws IOException, SolrServerException {
        return solrService.solrServerAdd(getSolrServer(coreName), doc);
    }

    public int optimizeIndex(String collection) throws IOException, SolrServerException {
        return solrService.solrServerOptimize(getSolrServer(collection));
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

    public int addInfoFiles(String coreName, JSONObject contents) throws IOException, SolrServerException {
        List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
        List<String> fieldsToDelete = new ArrayList<String>();

        Iterator<?> keys = contents.keys();

        while (keys.hasNext()) {
            final String title   = (String) keys.next();
            final String content = contents.getString(title);
            fieldsToDelete.add(title);

            HashMap<String, Object> params = new HashMap<String, Object>() {{
                put(title, content);
                put(FieldNames.TITLE, title);
                put(FieldNames.ID, UUID.nameUUIDFromBytes(title.getBytes()));
                put(FieldNames.URL, title);
            }};
            docs.add(createSolrDocument(params));
        }

        deleteByField(coreName, FieldNames.TITLE, fieldsToDelete);
        return update(coreName, docs);
    }


    private HashMap<String, Object> addParseMeta(HashMap<String, Object> params, Metadata metadata) {
        List<String> names = Arrays.asList(metadata.names());
        for (Map.Entry<String,String> entry : FieldNames.METADATA_TO_SOLRFIELDS.entrySet()) {
            if (names.contains(entry.getKey())) {
                params.put(entry.getValue(), metadata.get(entry.getKey()));
            }
        }
        return params;
    }

    private HashMap<String, Object> addSMBDates(HashMap<String, Object> params, String url) {
        boolean addLastModified = !params.containsKey(FieldNames.LAST_MODIFIED);
        boolean addCreateTime   = !params.containsKey(FieldNames.CREATE_DATE);

        if (addCreateTime || addLastModified) {
            try {
                SmbFile file = new SmbFile(url, getAuth());
                if (file.exists()) {
                    if (addLastModified) params.put(FieldNames.LAST_MODIFIED, DateField.formatExternal(new Date(file.getLastModified())));
                    if (addCreateTime)   params.put(FieldNames.CREATE_DATE, DateField.formatExternal(new Date(file.createTime())));
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
            put(FieldNames.HDFSKEY, urlString);
            put(FieldNames.HDFSSEGMENT, segment);
            put(FieldNames.BOOST, 1.0);
            put(FieldNames.ID, urlString);
            put(FieldNames.URL, urlString);
            put(FieldNames.TIMESTAMP, DateField.formatExternal(Calendar.getInstance().getTime()));
            put(FieldNames.CONTENT_TYPE, contentType);
        }};
        params = addParseMeta(params, parseData.getParseMeta());

        if (urlString.startsWith(Constants.SMB_PROTOCOL)) {
            params = addSMBDates(params, urlString);
        }

        if (!params.containsKey(FieldNames.TITLE)) {
            String[] urlItems = urlString.split("/");
            params.put(FieldNames.TITLE, urlItems[urlItems.length - 1]);
        }

        if (contentType.equals(Constants.JSON_CONTENT_TYPE)) {
            params = addFieldsFromJsonObject(params, content, "");
        } else if (!Utils.nullOrEmpty(content)) {
            params.put(FieldNames.CONTENT, content);
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

    public String getSchemaXML(String collectionName) {
        if (Utils.nullOrEmpty(collectionName)) return "";

        String uri = SolrUtils.getSolrZkSchemaURI(collectionName);
        String schemaStr = HttpClientUtils.httpGetRequest(uri);
        JSONObject schema = JSONUtils.convertStringToJSONObject(schemaStr);
        return (String) JSONUtils.extractJSONProperty(schema, Arrays.asList(Solr.ZK_NODE, "data"), String.class, "");
    }

    public JSONObject getCollectionInfo(String collectionName) {
        JSONObject collectionInfo = solrService.getCollectionInfo();
        return JSONUtils.getJSONObjectValue(collectionInfo, collectionName);
    }

    public int doSolrOperation(String collection, int operation, JSONObject infoFieldParams, StringWriter writer) {
        int success = Response.CODE_ERROR;
        String msg = Operations.OPERATION_MSGS.get(operation);

        writer.write("Performing operation " + msg.toLowerCase() + " on core " + collection + ".\n");

        try {
            if (operation == Operations.OPERATION_UPDATE) {
                success = update(collection);
            } else if (operation == Operations.OPERATION_DELETE) {
                success = deleteIndex(collection);
            } else if (operation == Operations.OPERATION_ADD_INFOFILES) {
                if (infoFieldParams != null) {
                    success = addInfoFiles(collection, infoFieldParams);
                } else {
                    writer.write("Did not update info field parameters for " + collection + "\n");
                }
            } else if (operation == Operations.OPERATION_OPTIMIZE) {
                success = optimizeIndex(collection);
            } else {
                writer.write("Invalid operation code: " + operation + "\n");
            }

            writer.write("\t" + msg + " success: " + Response.success(success) + "\n");
        } catch (SolrServerException e) {
            writer.write("\t" + msg + " failed with SolrServerException: " + e.getMessage() + "\n");
        } catch (IOException e) {
            writer.write("\t" + msg + " failed with IOException: " + e.getMessage() + "\n");
        }

        writer.write("\n");
        return success;
    }
}
