package model.schema;

import model.search.FacetFieldEntryList;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.util.*;

import static GatesBigData.constants.solr.FieldNames.*;
import static GatesBigData.constants.solr.Solr.*;
import static GatesBigData.constants.XmlConfig.*;
import static GatesBigData.utils.SolrUtils.*;
import static GatesBigData.utils.JSONUtils.*;
import static GatesBigData.utils.Utils.*;
import static GatesBigData.utils.HttpClientUtils.*;
import static GatesBigData.constants.Constants.*;

@Component
@Scope(value = "application")
public class CollectionsConfig {

    private HashMap<String, CollectionSchemaInfo> schemaInfoMap;
    private HashMap<String, CollectionConfig> collectionConfigMap;

    public CollectionsConfig() {
        schemaInfoMap       = new HashMap<String, CollectionSchemaInfo>();
        collectionConfigMap = new HashMap<String, CollectionConfig>();

        initializeSchemaMap();
        initializeCollectionConfigMap();
    }

    private void initializeCollectionConfigMap() {
        Document doc = getXmlDocumentFromFile(COLLECTION_CONF_FILE);
        if (doc == null) return;

        NodeList nodes = doc.getElementsByTagName(COLLECTION_TAG);

        for(int i = 0; i < nodes.getLength() ; i++){
            CollectionConfig config  = new CollectionConfig(nodes.item(i).getChildNodes());

            CollectionSchemaInfo schemaInfo = getSchema(config.getCollection());
            if (schemaInfo != null) {
                config.initializeFacetFieldEntryList(schemaInfo.getFacetFieldEntryList());
            }

            collectionConfigMap.put(config.getCollection(), config);
        }
    }

    private void initializeSchemaMap() {
        String configsStr = httpGetRequest(getSolrZkConfigsDirURI());
        if (nullOrEmpty(configsStr)) return;

        JSONObject configs = convertStringToJSONObject(configsStr);
        JSONArray children = (JSONArray) extractJSONProperty(configs, Arrays.asList("tree", "0", "children"), JSONArray.class, null);

        for(int i = 0; i < children.size(); i++) {
            String collection = children.getJSONObject(i).getJSONObject("data").getString("title");
            addSchema(collection);
        }
    }

    private void addSchema(String collection) {
        if (!nullOrEmpty(collection) && !schemaInfoMap.containsKey(collection)) {
            schemaInfoMap.put(collection, new CollectionSchemaInfo(collection));
        }
    }

    public CollectionSchemaInfo getSchema(String collection) {
        return schemaInfoMap.get(collection);
    }

    public boolean collectionHasStructuredData(String collection) {
        return schemaInfoMap.containsKey(collection) && schemaInfoMap.get(collection).isStructuredData();
    }

    public ReportData getReportData(String collection) {
        return collectionConfigMap.get(collection).getReportData();
    }

    public CollectionConfig getCollectionConfig(String collection) {
        return collectionConfigMap.get(collection);
    }

    public String getViewFieldsString(String collection) {
        CollectionConfig conf = getCollectionConfig(collection);
        List<String> viewFields = conf != null ? conf.getViewFields() : schemaInfoMap.get(collection).getViewFields();
        return StringUtils.join(viewFields, DEFAULT_DELIMETER);
    }

    public List<String> getSchemaViewFields(String collection) {
        return getSchema(collection).getViewFields();
    }

    public List<String> getFieldsByType(String collection, String type) {
        CollectionConfig conf = getCollectionConfig(collection);
        if (type.equals(VIEW_TYPE_AUDITVIEW)) {
            return conf.getFieldList(PROPERTY_FIELDS_AUDIT);
        } else if (type.equals(VIEW_TYPE_PREVIEW)) {
            return conf.getFieldList(PROPERTY_FIELDS_PREVIEW);
        }

        return getSchemaViewFields(collection);
    }

    public FacetFieldEntryList getFacetFieldEntryList(String collection) {
        return getCollectionConfig(collection).getFacetFieldEntryList();
    }

    public List<String> getFieldList(String collection, String key) {
        return getCollectionConfig(collection).getFieldList(key);
    }
}
