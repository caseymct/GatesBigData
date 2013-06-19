package model;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.utils.*;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;
import service.CoreService;
import service.SolrService;

import java.io.IOException;
import java.util.*;

@Component
@Scope(value = "session", proxyMode = ScopedProxyMode.TARGET_CLASS)
public class SolrSchemaInfo {
    private static HashMap<String, SolrCollectionSchemaInfo> schemaInfoHashMap;

    public SolrSchemaInfo() {
        schemaInfoHashMap = new HashMap<String, SolrCollectionSchemaInfo>();
        initialize();
    }

    private void initialize() {
        String configsUri = SolrUtils.getSolrZkConfigsDirURI();
        String configsStr = HttpClientUtils.httpGetRequest(configsUri);
        if (!Utils.nullOrEmpty(configsStr)) {
            JSONObject configs = JSONUtils.convertStringToJSONObject(configsStr);
            JSONArray children = (JSONArray) JSONUtils.extractJSONProperty(configs,
                    Arrays.asList("tree", "0", "children"), JSONArray.class, null);

            for(int i = 0; i < children.size(); i++) {
                String collection = (String) JSONUtils.extractJSONProperty(children.getJSONObject(i),
                        Arrays.asList("data", "title"), String.class, null);
                addSchema(collection);
            }
        }
    }

    private void addSchema(String collection) {
        if (!Utils.nullOrEmpty(collection) && !schemaInfoHashMap.containsKey(collection)) {
            schemaInfoHashMap.put(collection, new SolrCollectionSchemaInfo(collection));
        }
    }

    public SolrCollectionSchemaInfo getSchema(String collection) {
        if (!schemaInfoHashMap.containsKey(collection)) {
            addSchema(collection);
        }

        return schemaInfoHashMap.get(collection);
    }

    public List<String> getViewFieldNames(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getViewFieldNames() : new ArrayList<String>();
    }

    public String getViewFieldNamesString(String collectionName) {
        return StringUtils.join(getViewFieldNames(collectionName), Constants.DEFAULT_DELIMETER);
    }

    public Collection<String> getPrefixFieldCopySources(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getPrefixFieldCopySources() : new ArrayList<String>();
    }

    public String getPrefixFieldCopySourceString(String collectionName) {
        return StringUtils.join(getPrefixFieldCopySources(collectionName), Constants.DEFAULT_DELIMETER);
    }

    public String getCoreTitle(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getCoreTitle() : collectionName;
    }

    public boolean hasSuggestionCore(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) && schemaInfo.hasSuggestionCore();
    }

    public List<String> getPrefixTokenFields(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getPrefixTokenFields() : new ArrayList<String>();
    }

    public List<String> getFieldNamesSubset(String collectionName, List<String> indices, boolean include) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getFieldNamesSubset(indices, include) : new ArrayList<String>();
    }

    public boolean containsField(String collection, String field) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collection);
        return !Utils.nullOrEmpty(schemaInfo) && schemaInfo.containsField(field);
    }

    public boolean isFieldMultiValued (String collectionName, String field) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) && schemaInfo.isFieldMultiValued(field);
    }

    public boolean isStructuredData (String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) && schemaInfo.isStructuredData();
    }

    public Map<String, String> getPrefixFieldMap(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getPrefixFieldMap() : new HashMap<String, String>();
    }

    public String getSuggestionCore(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getSuggestionCore() : null;
    }

    public String getCorrespondingFacetFieldIfExists(String collectionName, String fieldName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getCorrespondingFacetFieldIfExists(fieldName) : fieldName;
    }

    public FacetFieldEntryList getFacetFieldEntryList(String collectionName) {
        SolrCollectionSchemaInfo schemaInfo = getSchema(collectionName);
        return !Utils.nullOrEmpty(schemaInfo) ? schemaInfo.getFacetFieldEntryList() : new FacetFieldEntryList();
    }
}
