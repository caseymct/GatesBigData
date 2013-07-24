package service;

import GatesBigData.constants.Constants;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
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

import static GatesBigData.constants.solr.Operations.*;
import static GatesBigData.constants.solr.Response.*;
import static GatesBigData.constants.solr.FieldNames.*;
import static GatesBigData.utils.JSONUtils.*;
import static GatesBigData.utils.Utils.*;

public class CollectionServiceImpl implements CollectionService {

    private SolrService solrService;
    private NtlmPasswordAuthentication auth;
    private static final Logger logger = Logger.getLogger(CollectionServiceImpl.class);

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

    private HashMap<String, Object> addParseMeta(HashMap<String, Object> params, Metadata metadata) {
        for (String name : Arrays.asList(metadata.names())) {
            String solrField = getSolrFieldFromMetaData(name);
            if (solrField != null) {
                params.put(solrField, metadata.get(name));
            }
        }
        return params;
    }

    private HashMap<String, Object> addSMBDates(HashMap<String, Object> params, String url) {
        boolean addLastModified = !params.containsKey(LAST_MODIFIED);
        boolean addCreateTime   = !params.containsKey(CREATION_DATE);

        if (addCreateTime || addLastModified) {
            try {
                SmbFile file = new SmbFile(url, getAuth());
                if (file.exists()) {
                    if (addLastModified) params.put(LAST_MODIFIED, DateField.formatExternal(new Date(file.getLastModified())));
                    if (addCreateTime)   params.put(CREATION_DATE, DateField.formatExternal(new Date(file.createTime())));
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
            put(HDFSKEY, urlString);
            put(HDFSSEGMENT, segment);
            put(BOOST, 1.0);
            put(ID, urlString);
            put(URL, urlString);
            put(TIMESTAMP, DateField.formatExternal(Calendar.getInstance().getTime()));
            put(CONTENT_TYPE, contentType);
        }};
        params = addParseMeta(params, parseData.getParseMeta());

        if (urlString.startsWith(SMB_PROTOCOL)) {
            params = addSMBDates(params, urlString);
        }

        if (!params.containsKey(TITLE)) {
            String[] urlItems = urlString.split("/");
            params.put(TITLE, urlItems[urlItems.length - 1]);
        }

        if (contentType.equals(JSON_CONTENT_TYPE)) {
            params = addFieldsFromJsonObject(params, content, "");
        } else if (!nullOrEmpty(content)) {
            params.put(CONTENT, content);
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
        JSONObject o = convertStringToJSONObject(jsonObjectStr);
        return o == null ? params : addFieldsFromJsonObject(params, o, parentStr);
    }

    public JSONObject getCollectionInfo(String collection) {
        return getJSONObjectValue(solrService.getCollectionInfo(), collection);
    }

    public int doSolrOperation(String collection, int operation, StringWriter writer) {
        int success = CODE_ERROR;
        String msg = OPERATION_MSGS.get(operation);

        writer.write("Performing operation " + msg.toLowerCase() + " on core " + collection + ".\n");

        try {
            if (operation == OPERATION_UPDATE) {
                success = update(collection);
            } else if (operation == OPERATION_DELETE) {
                success = deleteIndex(collection);
            } else if (operation == OPERATION_OPTIMIZE) {
                success = optimizeIndex(collection);
            } else {
                writer.write("Invalid operation code: " + operation + "\n");
            }

            writer.write("\t" + msg + " success: " + success(success) + "\n");
        } catch (SolrServerException e) {
            writer.write("\t" + msg + " failed with SolrServerException: " + e.getMessage() + "\n");
        } catch (IOException e) {
            writer.write("\t" + msg + " failed with IOException: " + e.getMessage() + "\n");
        }

        writer.write("\n");
        return success;
    }
}
