package model.schema;


import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.FieldTypes;
import GatesBigData.utils.*;
import model.search.FacetFieldEntryList;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.*;

import static GatesBigData.utils.HttpClientUtils.httpGetRequest;
import static GatesBigData.utils.JSONUtils.*;
import static GatesBigData.utils.SolrUtils.*;
import static GatesBigData.utils.Utils.*;
import static GatesBigData.constants.solr.Solr.*;
import static GatesBigData.constants.solr.FieldNames.*;

public class CollectionSchemaInfo {
    static final String COPYFIELD_SOURCE_KEY = "source";
    static final String COPYFIELD_DEST_KEY   = "dest";
    static final String SCHEMA_FIELD_KEY     = "field";
    static final String SCHEMA_COPYFIELD_KEY = "copyField";

    private Map<String, SchemaField> fieldInfoMap           = new HashMap<String, SchemaField>();
    private List<String> prefixTokens                       = new ArrayList<String>();
    private List<String> viewFields                         = new ArrayList<String>();
    private List<String> dateFieldNames                     = new ArrayList<String>();

    private Map<String, String> prefixFieldToCopySourceMap  = new HashMap<String, String>();
    private Map<String, String> copySourceToFacetFieldMap   = new HashMap<String, String>();
    private FacetFieldEntryList facetFieldEntryList         = new FacetFieldEntryList();
    private DynamicDateFields dynamicDateFields             = null;

    private String coreTitle        = "";
    private Document schema;
    private boolean structuredData  = true;
    private String suggestionCore   = "";

    private static Logger logger = Logger.getLogger(CollectionSchemaInfo.class);

    public CollectionSchemaInfo() {}

    public CollectionSchemaInfo(String collection) {
        this.schema = this.getSchemaXMLDocument(collection);
        parseSchema();

        this.dynamicDateFields = new DynamicDateFields(this.dateFieldNames);
        updateDynamicDateFieldDateRanges();
    }

    private Document getSchemaXMLDocument(String collection) {
        String schemaStr     = httpGetRequest(getSolrZkSchemaURI(collection));
        JSONObject schemaObj = convertStringToJSONObject(schemaStr);
        String schemaXMLString = (String) extractJSONProperty(schemaObj, Arrays.asList(ZK_NODE, DATA_NODE), String.class, "");
        return !nullOrEmpty(schemaXMLString) ? getXmlDocumentFromString(schemaXMLString) : null;
    }

    private void parseSchema() {
        if (nullOrEmpty(this.schema)) {
            return;
        }

        populateFieldInfo(this.schema.getElementsByTagName(SCHEMA_FIELD_KEY));
        populateCopyFieldInfo(this.schema.getElementsByTagName(SCHEMA_COPYFIELD_KEY));
    }

    public void populateFieldInfo(NodeList fields) {
        for(int s = 0; s < fields.getLength() ; s++){
            Node field = fields.item(s);
            if (field.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            SchemaField schemaField = new SchemaField(field.getAttributes());
            String fieldName = schemaField.getName();
            fieldInfoMap.put(fieldName, schemaField);

            if (schemaField.isCoreTitleField()) {
                coreTitle = schemaField.getDefaultValue();
            }

            if (schemaField.isStructuredDataField()) {
                structuredData = Boolean.parseBoolean(schemaField.getDefaultValue());
            }

            if (schemaField.isSuggestionCoreField()) {
                suggestionCore = schemaField.getDefaultValue();
            }

            if (!ignoreFieldName(fieldName)) {
                facetFieldEntryList.add(fieldName, schemaField.getType(), schemaField.isMultiValued());
                viewFields.add(fieldName);
            }

            if (schemaField.isDateType()) {
                dateFieldNames.add(fieldName);
            }

            if (schemaField.isPrefixTokenType()) {
                prefixTokens.add(fieldName);
            }
        }
    }

    public void populateCopyFieldInfo(NodeList copyFields) {
        for(int s = 0; s < copyFields.getLength() ; s++){
            Node field = copyFields.item(s);
            if (field.getNodeType() == Node.ELEMENT_NODE){
                Element el = (Element) field;
                String src  = el.getAttribute(COPYFIELD_SOURCE_KEY);
                String dest = el.getAttribute(COPYFIELD_DEST_KEY);

                if (src.equals(FieldNames.CONTENT)) continue;

                if (dest.endsWith(FieldNames.PREFIX_SUFFIX)) {
                    prefixFieldToCopySourceMap.put(dest, el.getAttribute(COPYFIELD_SOURCE_KEY));
                } else if (dest.endsWith(FACET_SUFFIX)) {
                    copySourceToFacetFieldMap.put(el.getAttribute(COPYFIELD_SOURCE_KEY), dest);
                }
            }
        }
    }

    public boolean hasDynamicDateFieldDateRanges() {
        return this.dynamicDateFields.hasDateRanges();
    }

    public void updateDynamicDateFieldDateRanges() {
        if (!hasSuggestionCore()) {
            return;
        }

        SolrServer server = new HttpSolrServer(getSolrServerURI(suggestionCore));
        try {
            QueryResponse rsp = server.query(dynamicDateFields.getDateSolrQuery());
            if (rsp != null) {
                this.dynamicDateFields.update(rsp.getResults());
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }
    }

    public List<Date> getDateRange(String f) {
        if (!this.dynamicDateFields.hasDateRangeForField(f)) {
            updateDynamicDateFieldDateRanges();
        }

        Date start = getDateRangeStart(f);
        Date end   = getDateRangeEnd(f);
        return (start != null && end != null) ? Arrays.asList(start, end) : null;
    }

    public Date getDateRangeStart(String f) {
        return this.dynamicDateFields.getStart(f);
    }

    public Date getDateRangeEnd(String f) {
        return this.dynamicDateFields.getEnd(f);
    }

    public boolean containsField(String fieldName) {
        return fieldInfoMap.containsKey(fieldName);
    }

    public boolean isFieldMultiValued(String fieldName) {
        return fieldInfoMap.get(fieldName).isMultiValued();
    }

    public boolean isStructuredData() {
        return structuredData;
    }

    public String getCoreTitle() {
        return coreTitle;
    }

    public boolean hasSuggestionCore() {
        return !Utils.nullOrEmpty(this.suggestionCore);
    }

    public String getSuggestionCore() {
        return suggestionCore;
    }

    public String getSortFieldIfValid(String f) {
        return containsField(f) && !isFieldMultiValued(f) ? f : null;
    }

    public List<String> getFieldNamesByType(String type) {
        List<String> fieldsByType = new ArrayList<String>();

        for(SchemaField field : fieldInfoMap.values()) {
            if (field.getType().equals(type)) {
                fieldsByType.add(field.getName());
            }
        }
        Collections.sort(fieldsByType);
        return fieldsByType;
    }

    public SchemaField getSchemaField(String fieldName) {
        return fieldInfoMap.get(fieldName);
    }

    public String getCorrespondingFacetFieldIfExists(String fieldName) {
        return copySourceToFacetFieldMap.containsKey(fieldName) ? copySourceToFacetFieldMap.get(fieldName) : fieldName;
    }

    public String getFieldType(String fieldName) {
        return fieldInfoMap.containsKey(fieldName) ? fieldInfoMap.get(fieldName).getType() : null;
    }

    public boolean fieldTypeIsDate(String fieldName) {
        SchemaField schemaField = getSchemaField(fieldName);
        return schemaField != null && schemaField.getType().equals(FieldTypes.DATE);
    }

    public boolean fieldTypeIsString(String fieldName) {
        SchemaField schemaField = getSchemaField(fieldName);
        return schemaField != null && FieldTypes.TEXT_FIELDS.contains(schemaField.getType());
    }

    public boolean fieldTypeIsNumber(String fieldName) {
        SchemaField schemaField = getSchemaField(fieldName);
        return schemaField != null && FieldTypes.NUMBER_FIELDS.contains(schemaField.getType());
    }

    public List<String> getViewFields() {
        return viewFields;
    }

    public Map<String, String> getPrefixFieldMap() {
        return prefixFieldToCopySourceMap;
    }

    public Collection<String> getPrefixFieldCopySources() {
        return prefixFieldToCopySourceMap.values();
    }

    public List<String> getDateFieldNames() {
        return dateFieldNames;
    }

    public List<String> getPrefixTokenFields() {
        return prefixTokens;
    }

    public FacetFieldEntryList getFacetFieldEntryList() {
        return facetFieldEntryList;
    }

    public List<String> getFieldNamesSubset(List<String> indices, boolean include) {
        List<String> subList = new ArrayList<String>();
        List<String> fields = new ArrayList<String>(viewFields);

        for(int i = 0; i < fields.size(); i++) {
            if ((include && indices.contains(i + "")) || (!include && !indices.contains(i + ""))) {
                subList.add(fields.get(i));
            }
        }
        return subList;
    }
}
