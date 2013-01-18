package service;

import LucidWorksApp.utils.*;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.DateField;

import org.springframework.beans.factory.annotation.Autowired;

import javax.swing.*;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.*;

public class CoreServiceImpl implements CoreService {

    private SolrService solrService;
    private static String LAST_MODIFIED_FIELD_NAME      = "last_modified";
    private static String CREATE_DATE_FIELD_NAME        = "creation_date";
    private static String LAST_AUTHOR_FIELD_NAME        = "last_author";
    private static String APPLICATION_NAME_FIELD_NAME   = "application_name";
    private static String AUTHOR_FIELD_NAME             = "author";
    private static String COMPANY_FIELD_NAME            = "company";
    private static String TITLE_FIELD_NAME              = "title";

    private NtlmPasswordAuthentication auth;
    private static final Logger logger = Logger.getLogger(CoreServiceImpl.class);

    HashMap<String, String> mapMetadataToSolrFields = new HashMap<String, String>() {{
        put("Last-Save-Date", LAST_MODIFIED_FIELD_NAME);
        put("Creation-Date", CREATE_DATE_FIELD_NAME);
        put("Last-Author", LAST_AUTHOR_FIELD_NAME);
        put("Application-Name", APPLICATION_NAME_FIELD_NAME);
        put("Author", AUTHOR_FIELD_NAME);
        put("Company", COMPANY_FIELD_NAME);
        put("title", TITLE_FIELD_NAME);
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

    public boolean deleteIndex(String coreName) {
        return deleteByField(coreName, "*", "*");
        /*
        try {
            SolrServer server = getSolrServer(coreName);
            server.deleteByQuery("*:*");
            solrService.solrServerCommit(server);
            return true;

        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return false; */
    }

    public boolean deleteById(String coreName, List<String> ids) {
        try {
            SolrServer server = getSolrServer(coreName);
            server.deleteById(ids);
            solrService.solrServerCommit(server);
            return true;

        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return false;
    }

    public boolean deleteByField(String coreName, String field, String value) {
        return deleteByField(coreName, field, Arrays.asList(value));
    }

    public boolean deleteByField(String coreName, String field, List<String> values) {
        try {
            SolrServer server = getSolrServer(coreName);
            for(String value : values) {
                server.deleteByQuery(field + ":" + value);
            }
            solrService.solrServerCommit(server);
            return true;

        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return false;
    }

    public boolean addDocumentToSolrIndex(SolrInputDocument doc, String coreName) {
        return solrService.solrServerCommit(getSolrServer(coreName), Arrays.asList(doc));
    }

    public boolean addDocumentsToSolrIndex(List<SolrInputDocument> docs, String coreName) {
        return solrService.solrServerCommit(getSolrServer(coreName), docs);
    }

    public SolrInputDocument createSolrDocument(HashMap<String, String> params) {
        SolrInputDocument doc = new SolrInputDocument();

        for(Map.Entry<String, String> entry : params.entrySet()) {
            doc.addField(entry.getKey(), entry.getValue());
        }

        return doc;
    }

    public boolean addInfoFilesToSolr(String coreName, HashMap<String, String> hdfsInfoFileContents) {
        List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

        for(Map.Entry<String, String> entry : hdfsInfoFileContents.entrySet()) {
            String title = entry.getKey();
            String content = entry.getValue();
            String id = UUID.randomUUID().toString();

            HashMap<String, String> params = new HashMap<String, String>();
            params.put(title, content);
            params.put(Constants.SOLR_TITLE_FIELD_NAME, title);
            params.put(Constants.SOLR_ID_FIELD_NAME, id);
            params.put(Constants.SOLR_URL_FIELD_NAME, title);
            docs.add(createSolrDocument(params));
        }

        deleteByField(coreName, Constants.SOLR_TITLE_FIELD_NAME, Constants.SOLR_INFO_FILES_LIST);
        return addDocumentsToSolrIndex(docs, coreName);
    }


    public boolean createAndAddDocumentToSolr(Object content, String hdfsKey, String coreName) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(SolrUtils.SOLR_SCHEMA_HDFSKEY, hdfsKey);

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

        return addDocumentToSolrIndex(doc, coreName);
    }

    private SolrInputDocument addParseMeta(Metadata metadata, SolrInputDocument doc) {
        List<String> names = Arrays.asList(metadata.names());
        for (Map.Entry<String,String> entry : mapMetadataToSolrFields.entrySet()) {
            if (names.contains(entry.getKey())) {
                doc.addField(entry.getValue(), metadata.get(entry.getKey()));
            }
        }
        return doc;
    }

    private SolrInputDocument addFieldIfNull(SolrInputDocument doc, String field, Object value) {
        if (doc.getField(field) == null) {
            doc.addField(field, value);
        }
        return doc;
    }

    private SolrInputDocument addSMBDatesToDoc(SolrInputDocument doc, String url) {
        try {
            SmbFile file = new SmbFile(url, getAuth());
            if (file.exists()) {
                doc = addFieldIfNull(doc, LAST_MODIFIED_FIELD_NAME, DateUtils.formatToSolr(new Date(file.getLastModified())));
                doc = addFieldIfNull(doc, CREATE_DATE_FIELD_NAME, DateUtils.formatToSolr(new Date(file.createTime())));
            }
        } catch (SmbException e) {
            logger.error(e.getMessage());
        } catch (MalformedURLException e){
            logger.error(e.getMessage());
        }
        return doc;
    }

    public SolrInputDocument createSolrInputDocumentFromNutch(String urlString, ParseData parseData, String segment, String coreName,
                                                              String contentType, String content) {
        if (parseData == null || !parseData.getStatus().isSuccess()) {
            return null;
        }

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("HDFSKey", urlString);
        doc.addField("HDFSSegment", segment);
        doc.addField("boost", 1.0);
        doc.addField("id", urlString);
        doc.addField("url", urlString);
        doc.addField("tstamp", DateUtils.formatToSolr(Calendar.getInstance().getTime()));
        doc.addField("content_type", contentType);
        doc = addParseMeta(parseData.getParseMeta(), doc);

        if (urlString.startsWith(Constants.SMB_PROTOCOL_STRING) &&
                (doc.getField(LAST_MODIFIED_FIELD_NAME) == null || doc.getField(CREATE_DATE_FIELD_NAME) == null)) {
            doc = addSMBDatesToDoc(doc, urlString);
        }

        if (doc.getField(TITLE_FIELD_NAME) == null) {
            String[] urlItems = urlString.split("/");
            doc.addField(TITLE_FIELD_NAME, urlItems[urlItems.length - 1]);
        }

        if (contentType.equals(Constants.JSON_CONTENT_TYPE)) {
            try {
                JSONObject jsonObject = JSONObject.fromObject(content);
                addFieldsFromJsonObject(doc, jsonObject, "");
            } catch (JSONException e) {
                logger.error("Invalid json for file: " + urlString);
            }
        } else if (!Utils.nullOrEmpty(content)) {
            doc.addField("content", content);
        }

        return doc;
    }

    public boolean addNutchDocumentToSolr(String urlString, ParseData parseData, String segment,
                                          String coreName, String contentType, String content) {
        SolrInputDocument doc = createSolrInputDocumentFromNutch(urlString, parseData, segment, coreName, contentType, content);
        return (doc != null) && addDocumentToSolrIndex(doc, coreName);
    }

    private SolrInputDocument addFieldsFromJsonObject(SolrInputDocument doc, JSONObject jsonObject, String parentStr) {
        for(Object key : jsonObject.keySet()) {
            if (jsonObject.get(key) instanceof JSONObject) {
                doc = addFieldsFromJsonObject(doc, jsonObject.getJSONObject(key.toString()), key + ".");
            } else {
                doc.addField(parentStr + key, jsonObject.get(key));
            }
        }
        return doc;
    }

    private String getDateGapString(String startString, String endString, int buckets) {
        try {
            long msStart = DateField.parseDate(startString).getTime();
            long msEnd   = DateField.parseDate(endString).getTime();
            return DateUtils.getDateGapString((msEnd - msStart)/buckets);
        } catch (ParseException e) {
            logger.error(e.getMessage());
        }
        return "";
    }

    private String getSolrDate(SolrServer server, String field, String format, SolrQuery.ORDER order) {
        SolrQuery query = new SolrQuery();
        query.setQuery(field + ":*");
        query.setStart(0);
        query.setRows(1);
        query.addSortField(field, order);
        query.setFields(field);
        query = SolrUtils.setResponseFormatAsJSON(query);

        try {
            QueryResponse rsp = server.query(query);
            SolrDocumentList docs = rsp.getResults();
            if (docs.size() > 0) {
                SolrDocument doc = docs.get(0);
                if (doc.containsKey(field)) {
                    Object val = doc.getFieldValue(field);
                    if (val instanceof Date) {
                        String solrDate = DateUtils.getSolrDate(val.toString());
                        return DateUtils.getFormattedDateStringFromSolrDate(solrDate, format);
                    }
                }
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }

        return "";
    }

    public List<String> getSolrFieldDateRange(String coreName, String field, String format) {
        List<String> dateRange = new ArrayList<String>();
        int buckets = 10;
        SolrServer server = getSolrServer(coreName);

        dateRange.add(getSolrDate(server, field, format, SolrQuery.ORDER.asc));
        dateRange.add(getSolrDate(server, field, format, SolrQuery.ORDER.desc));

        if (format.equals(DateUtils.SOLR_DATE)) {
            dateRange.add(getDateGapString(dateRange.get(0), dateRange.get(1), buckets));
        }
        return dateRange;
    }
}
