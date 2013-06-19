package model;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.FieldTypes;
import GatesBigData.constants.solr.Solr;
import GatesBigData.utils.*;
import net.sf.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.*;


public class SolrCollectionSchemaInfo {
    static final String COPYFIELD_SOURCE_KEY = "source";
    static final String COPYFIELD_DEST_KEY   = "dest";
    static final String SCHEMA_FIELD_KEY     = "field";
    static final String SCHEMA_COPYFIELD_KEY = "copyField";

    private Map<String, SchemaField> fieldInfoMap           = new HashMap<String, SchemaField>();
    private List<String> prefixTokens                       = new ArrayList<String>();
    private List<String> viewFieldNames                     = new ArrayList<String>();
    private List<String> dateFieldNames                     = new ArrayList<String>();

    private Map<String, String> prefixFieldToCopySourceMap  = new HashMap<String, String>();
    private Map<String, String> copySourceToFacetFieldMap   = new HashMap<String, String>();
    private FacetFieldEntryList facetFieldEntryList         = new FacetFieldEntryList();
    private DynamicDateFields dynamicDateFields             = null;

    private String coreTitle        = "";
    private String schema           = "";
    private boolean structuredData  = true;
    private String suggestionCore   = "";

    private static Logger logger = Logger.getLogger(SolrCollectionSchemaInfo.class);

    public SolrCollectionSchemaInfo() {}

    public SolrCollectionSchemaInfo(String collection) {
        setSchemaXML(collection);
        parseSchema();

        updateDynamicDateFieldDateRanges();
    }

    private void setSchemaXML(String collection) {
        if (Utils.nullOrEmpty(collection)) {
            return;
        }

        String uri           = SolrUtils.getSolrZkSchemaURI(collection);
        String schemaStr     = HttpClientUtils.httpGetRequest(uri);
        JSONObject schemaObj = JSONUtils.convertStringToJSONObject(schemaStr);
        schema = (String) JSONUtils.extractJSONProperty(schemaObj, Arrays.asList(Solr.ZK_NODE, "data"), String.class, "");
    }

    private void parseSchema() {
        if (Utils.nullOrEmpty(schema)) {
            return;
        }
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(IOUtils.toInputStream(schema));
            doc.getDocumentElement().normalize();

            populateFieldInfo(doc.getElementsByTagName(SCHEMA_FIELD_KEY));
            populateCopyFieldInfo(doc.getElementsByTagName(SCHEMA_COPYFIELD_KEY));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
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

            if (FieldNames.validFieldName(fieldName)) {
                facetFieldEntryList.add(fieldName, schemaField.getType(), schemaField.isMultiValued());
            }

            if (!FieldNames.ignoreFieldName(fieldName)) {
                viewFieldNames.add(fieldName);
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
                } else if (dest.endsWith(FieldNames.FACET_SUFFIX)) {
                    copySourceToFacetFieldMap.put(el.getAttribute(COPYFIELD_SOURCE_KEY), dest);
                }
            }
        }
    }

    public boolean hasDynamicDateFieldDateRanges() {
        return this.dynamicDateFields.hasDateRanges();
    }

    public void updateDynamicDateFieldDateRanges() {
        this.dynamicDateFields = new DynamicDateFields(this.dateFieldNames);

        if (hasSuggestionCore()) {
            SolrServer server = new HttpSolrServer(SolrUtils.getSolrServerURI(suggestionCore));
            try {
                QueryResponse rsp = server.query(dynamicDateFields.getDateSolrQuery());
                if (rsp != null) {
                    this.dynamicDateFields.update(rsp.getResults());
                }
            } catch (SolrServerException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public List<Date> getDateRange(String f) {
        return Arrays.asList(getDateRangeStart(f), getDateRangeEnd(f));
    }

    public Date getDateRangeStart(String f) {
        return this.dynamicDateFields.getStart(f);
    }

    public Date getDateRangeEnd(String f) {
        return this.dynamicDateFields.getEnd(f);
    }

    public ExtendedSolrQuery getDynamicDateFieldsQuery() {
        return this.dynamicDateFields.getDateSolrQuery();
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

    public List<String> getViewFieldNames() {
        return viewFieldNames;
    }

    public String getViewFieldNamesString() {
        return StringUtils.join(viewFieldNames, Constants.DEFAULT_DELIMETER);
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

        for(int i = 0; i < viewFieldNames.size(); i++) {
            if ((include && indices.contains(i + "")) || (!include && !indices.contains(i + ""))) {
                subList.add(viewFieldNames.get(i));
            }
        }
        return subList;
    }
}
