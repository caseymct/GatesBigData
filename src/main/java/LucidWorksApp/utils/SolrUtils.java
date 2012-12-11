package LucidWorksApp.utils;


import model.FacetFieldEntryList;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.http.client.HttpClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.schema.SchemaField;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolrUtils {

    static final String LUKE_COPYSOURCES_KEY    = "copySources";
    static final String LUKE_SCHEMA_KEY         = "schema";
    static final String LUKE_FIELDS_KEY         = "fields";
    static final String LUKE_TYPE_KEY           = "type";

    public static final String SOLR_ENDPOINT         = "solr";
    public static final String SOLR_SUGGEST_ENDPOINT = "suggest";
    public static final String SOLR_SELECT_ENDPOINT  = "select";
    public static final String UPDATECSV_ENDPOINT    = "update/csv";
    public static final String LUKE_ENDPOINT         = "admin/luke";


    public static final String SOLR_SCHEMA_HDFSKEY              = "HDFSKey";
    public static final String SOLR_SCHEMA_HDFSSEGMENT          = "HDFSSegment";
    public static final String SOLR_SCHEMA_PREFIXFIELD_ENDSWITH = "Prefix";

    static Pattern COPYSOURCE_PATTERN = Pattern.compile("^" + SchemaField.class.getName() + ":(.*)\\{.*");

    static Pattern VIEW_FIELD_NAMES_PATTERN = Pattern.compile(".*(\\.facet|Suggest|Prefix)$");

    static Pattern FIELDNAMES_TOIGNORE = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
            "HDFSKey|boost|digest|host|segment|tstamp).*|.*id");


    public static boolean validFieldName(String fieldName, boolean isFacetField) {
        Matcher m = FIELDNAMES_TOIGNORE.matcher(fieldName);
        return (!isFacetField && fieldName.equals(SOLR_SCHEMA_HDFSKEY)) || !m.matches();
    }

    public static String getSolrServerURI(String coreName) {
        return Utils.addToUrlIfNotEmpty(Constants.SOLR_SERVER + "/" + SOLR_ENDPOINT, coreName);
    }

    public static String getSolrSuggestURI(String fieldSpecificEndpoint, String coreName, HashMap<String,String> urlParams) {
        String uri = getSolrServerURI(coreName) + "/" + SOLR_SUGGEST_ENDPOINT;
        return Utils.addToUrlIfNotEmpty(uri, fieldSpecificEndpoint) + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrSelectURI(String coreName) {
        return new Path(getSolrServerURI(coreName), SOLR_SELECT_ENDPOINT).toString();
    }

    public static String getSolrSelectURI(String coreName, HashMap<String,String> urlParams) {
        return getSolrSelectURI(coreName) + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrSelectURI(String coreName, HashMap<String,String> urlParams, HashMap<String, List<String>> repeatKeyUrlParams) {
        return getSolrSelectURI(coreName) + Utils.constructUrlParams(urlParams, repeatKeyUrlParams);
    }

    public static String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams) {
        return getSolrServerURI(coreName) + "/" + UPDATECSV_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public static String getLukeEndpoint(String coreName, HashMap<String, String> urlParams) {
        return getSolrServerURI(coreName) + "/" + LUKE_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public static SolrQuery.ORDER getSortOrder(String sortOrder) {
        return sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }

    public static String getDocumentContentType(SolrDocument doc) {
        return getFieldValue(doc, Constants.SOLR_CONTENT_TYPE_FIELD_NAME, Constants.TEXT_CONTENT_TYPE);
    }

    public static String getFieldValue(SolrDocument doc, String fieldName, String defaultValue) {
        if (doc.containsKey(fieldName)) {
            return (String) doc.get(fieldName);
        }
        return defaultValue;
    }

    public static FacetFieldEntryList getFacetFieldsFromLukeFieldsObject(JSONObject fields) {
        FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();

        try {
            Iterator<?> keys = fields.keys();

            while(keys.hasNext()){
                String key = (String)keys.next();
                if (validFieldName(key, true)) {
                    JSONObject fieldInfo = fields.getJSONObject(key);
                    if (fieldInfo.containsKey(LUKE_TYPE_KEY) && !(fieldInfo.get(LUKE_TYPE_KEY) instanceof JSONNull) &&
                            fieldInfo.containsKey(LUKE_SCHEMA_KEY) && !(fieldInfo.get(LUKE_SCHEMA_KEY) instanceof JSONNull)) {
                        facetFieldEntryList.add(key, fieldInfo.getString(LUKE_TYPE_KEY), fieldInfo.getString(LUKE_SCHEMA_KEY));
                    }
                }
            }

        } catch (JSONException e) {
            System.err.println(e.getMessage());
        }
        return facetFieldEntryList;
    }

    private static List<String> getCopySourcesFieldsFromLukeFieldsObject(JSONObject fields, String fieldName) {
        List<String> copySourcesFieldNames = new ArrayList<String>();

        if (fields.containsKey(fieldName)) {
            JSONObject field = fields.getJSONObject(fieldName);

            if (field.containsKey(LUKE_COPYSOURCES_KEY)) {
                JSONArray copyFields = field.getJSONArray(LUKE_COPYSOURCES_KEY);

                for(int i = 0; i < copyFields.size(); i++) {
                    Matcher m = COPYSOURCE_PATTERN.matcher(copyFields.getString(i));
                    if (m.matches()) {
                        copySourcesFieldNames.add(m.group(1));
                    }
                }
            }
        }

        return copySourcesFieldNames;
    }

    public static HashMap<String, String> getPrefixFieldMap(String coreName) {
        JSONObject fields = getLukeFieldsObject(coreName, true);
        HashMap<String, String> prefixFieldMap = new HashMap<String, String>();

        for(Object fieldObject : fields.names()) {
            String fieldName = fieldObject.toString();
            if (fieldName.endsWith(SolrUtils.SOLR_SCHEMA_PREFIXFIELD_ENDSWITH)) {
                String copySourcesField = getCopySourcesFieldsFromLukeFieldsObject(fields, fieldName).get(0);
                if (!copySourcesField.equals("")) {
                    prefixFieldMap.put(fieldName, copySourcesField);
                }
            }
        }

        return prefixFieldMap;
    }

    public static JSONObject getLukeFieldsObject(String coreName, boolean showSchema) {
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put(Constants.SOLR_NUMTERMS_PARAM, "0");
        urlParams.put(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);

        if (showSchema) {
            urlParams.put(Constants.SOLR_SHOWSCHEMA_PARAM, Constants.SOLR_SHOWSCHEMA_DEFAULT);
        }

        String url = getLukeEndpoint(coreName, urlParams);
        String response = HttpClientUtils.httpGetRequest(url);
        try {
            JSONObject jsonObject = JSONObject.fromObject(response);
            JSONObject fieldsParent = jsonObject;

            if (showSchema && jsonObject.containsKey(LUKE_SCHEMA_KEY)) {
                fieldsParent = jsonObject.getJSONObject(LUKE_SCHEMA_KEY);
            }
            if (fieldsParent.containsKey(LUKE_FIELDS_KEY)) {
                return fieldsParent.getJSONObject(LUKE_FIELDS_KEY);
            }
        } catch (JSONException e) {
            System.err.println(e.getMessage());
        }
        return new JSONObject();
    }

    public static FacetFieldEntryList getLukeFacetFieldEntryList(String coreName) {
        return getFacetFieldsFromLukeFieldsObject(getLukeFieldsObject(coreName, false));
    }

    public static List<String> getLukeFacetFieldNames(String coreName) {
        return getFacetFieldsFromLukeFieldsObject(getLukeFieldsObject(coreName, false)).getNames();
    }

    public static List<String> getLukeFieldNames(String coreName) {
        return JsonParsingUtils.convertJSONArrayToStringList(getLukeFieldsObject(coreName, false).names());
    }

    public static JSONArray getLukeFieldNamesAsJSONArray(String coreName) {
        return getLukeFieldsObject(coreName, false).names();
    }

    public static List<String> getLukeFieldNamesByType(String coreName, String type) {
        List<String> fieldsByType = new ArrayList<String>();

        JSONObject fields = getLukeFieldsObject(coreName, false);
        for(Object fieldObj : fields.keySet()) {
            String field = (String) fieldObj;
            if (fields.getJSONObject(field).getString("type").equals(type)) {
                fieldsByType.add(field);
            }
        }
        return fieldsByType;
    }

    public static List<String> getViewFieldNames(String coreName) {
        List<String> ret = new ArrayList<String>();

        for(String field : getLukeFieldNames(coreName)) {
            Matcher m = VIEW_FIELD_NAMES_PATTERN.matcher(field);
            if (!m.matches()) {
                ret.add(field);
            }
        }
        return ret;
    }

    public static List<String> getLukeFieldNames(String coreName, List<String> indices, boolean include) {
        List<String> fieldNames = getLukeFieldNames(coreName);
        List<String> subList = new ArrayList<String>();

        for(int i = 0; i < fieldNames.size(); i++) {
            String s = i + "";
            if ((include && indices.contains(s)) || (!include && !indices.contains(s))) {
                subList.add(fieldNames.get(i));
            }
        }
        return subList;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for (SolrDocument doc : docs) {
            String hdfsSeg = (String) doc.getFieldValue(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add((String) doc.getFieldValue(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(JSONArray docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for(int i = 0; i < docs.size(); i++) {
            JSONObject doc = docs.getJSONObject(i);
            String hdfsSeg = doc.getString(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add(doc.getString(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }

    public static HttpSolrServer getHttpSolrServer(JobConf job) throws MalformedURLException {
        HttpClient client = new DefaultHttpClient();

        /* Check for username/password
        if (job.getBoolean(SolrConstants.USE_AUTH, false)) {
            String username = job.get(SolrConstants.USERNAME);
            AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthScope.ANY_SCHEME);

            client.getState().setCredentials(scope, new UsernamePasswordCredentials(username, job.get(SolrConstants.PASSWORD)));

            HttpClientParams params = client.getParams();
            params.setAuthenticationPreemptive(true);

            client.setParams(params);
        }
        */
        return new HttpSolrServer(job.get(SolrConstants.SERVER_URL), client);
    }

    public static CloudSolrServer getCloudSolrServer() {
        try {
            CloudSolrServer cloudSolrServer = new CloudSolrServer(Constants.ZOOKEEPER_SERVER);
            return cloudSolrServer;
        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
