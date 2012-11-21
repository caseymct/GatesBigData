package service;


import LucidWorksApp.utils.*;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import model.FacetFieldEntryList;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
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
import java.util.*;

public class CoreServiceImpl implements CoreService {

    private SolrService solrService;

    private static final String SMB_PROTOCOL_STRING = "smb://";
    private static final String SMB_DOMAIN          = "NA";
    private static final String SMB_USERNAME        = "bigdatasvc";
    private static final String SMB_PASSWORD        = "Crawl2012";

    private NtlmPasswordAuthentication auth;
    private static final Logger logger = Logger.getLogger(CoreServiceImpl.class);

    HashMap<String, String> mapMetadataToSolrFields = new HashMap<String, String>() {{
        put("Last-Save-Date", "last_modified");
        put("Creation-Date", "creation_date");
        put("Last-Author", "last_author");
        put("Application-Name", "application_name");
        put("Author", "author");
        put("Company", "company");
        put("Company", "company");
        put("title", "title");
    }};

    @Autowired
    public void setServices(SolrService solrService) {
        this.solrService = solrService;
    }

    private NtlmPasswordAuthentication getAuth() {
        if (this.auth == null) {
            this.auth = new NtlmPasswordAuthentication(SMB_DOMAIN, SMB_USERNAME, SMB_PASSWORD);
        }
        return this.auth;
    }

    public SolrServer getSolrServer(String coreName) {
        try {
            return new CommonsHttpSolrServer(SolrUtils.getSolrServerURI(coreName));

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    private LukeResponse.FieldInfo getLukeFieldInfo(SolrServer server, String fieldName) {
        LukeRequest luke = new LukeRequest();
        try {
            return luke.process(server).getFieldInfo(fieldName);
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public boolean isFieldMultiValued(SolrServer server, String fieldName) {
        LukeResponse.FieldInfo fieldInfo = getLukeFieldInfo(server, fieldName);
        return fieldInfo != null && fieldInfo.getSchema().charAt(3) == 77;
    }

    public boolean fieldExists(SolrServer server, String fieldName) {
        return getLukeFieldInfo(server, fieldName) != null;
    }

    public boolean deleteIndex(String coreName) {

        try {
            SolrServer server = getSolrServer(coreName);
            server.deleteByQuery("*:*");
            solrService.solrServerCommit(server);

        } catch (SolrServerException e) {
            logger.error(e.getMessage());
            return false;

        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }

        return true;
    }

    public boolean addDocumentToSolrIndex(SolrInputDocument doc, String coreName) {
        SolrServer solrServer = getSolrServer(coreName);
        return solrService.solrServerCommit(solrServer, Arrays.asList(doc));
    }

    public boolean addDocumentToSolrIndex(List<SolrInputDocument> docs, String coreName) {
        SolrServer solrServer = getSolrServer(coreName);
        return solrService.solrServerCommit(solrServer, docs);
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

    private SolrInputDocument addFieldIfNotNull(SolrInputDocument doc, String field, Object value) {
        if (doc.getField(field) == null) {
            doc.addField(field, value);
        }
        return doc;
    }

    private SolrInputDocument addSMBDatesToDoc(SolrInputDocument doc, String url) {
        try {
            SmbFile file = new SmbFile(url, getAuth());
            if (file.exists()) {
                doc = addFieldIfNotNull(doc, "last_modified", DateUtils.formatToSolr(new Date(file.getLastModified())));
                doc = addFieldIfNotNull(doc, "creation_date", DateUtils.formatToSolr(new Date(file.createTime())));
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

        if (urlString.startsWith(SMB_PROTOCOL_STRING) &&
                (doc.getField("last_modified") == null || doc.getField("creation_date") == null)) {
            doc = addSMBDatesToDoc(doc, urlString);
        }

        if (doc.getField("title") == null) {
            String[] urlItems = urlString.split("/");
            doc.addField("title", urlItems[urlItems.length - 1]);
        }

        if (contentType.equals(Constants.JSON_CONTENT_TYPE)) {
            try {
                JSONObject jsonObject = JSONObject.fromObject(content);
                addFieldsFromJsonObject(doc, jsonObject, "");
            } catch (JSONException e) {
                logger.error("Invalid json for file: " + urlString);
            }
        } else if (!Utils.stringIsNullOrEmpty(content)) {
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
        String date = (String) JsonParsingUtils.extractJSONProperty(JSONObject.fromObject(response),
                                    Arrays.asList("response", "docs", "0", field), String.class, "");
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

        String date = parseDateFromSolrResponse(SolrUtils.getSolrSelectURI(coreName, urlParams), field);
        dateRange.add(DateUtils.getFormattedDateStringFromSolrDate(date, format));

        urlParams.put("sort", field + "+desc");
        date = parseDateFromSolrResponse(SolrUtils.getSolrSelectURI(coreName, urlParams), field);
        dateRange.add(DateUtils.getFormattedDateStringFromSolrDate(date, format));

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
