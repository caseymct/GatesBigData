package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.HttpClientUtils;
import GatesBigData.utils.SolrUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
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

    private Map<String, SchemaField> fieldInfoMap = new HashMap<String, SchemaField>();
    private List<String> fieldNames;
    private List<String> viewFieldNames = new ArrayList<String>();
    private List<String> dateFieldNames = new ArrayList<String>();
    private Map<String, String> prefixFieldToCopySourceMap = new HashMap<String, String>();
    private FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();
    private String coreTitle;
    private boolean structuredData;
    private String suggestionCore;

    private static Logger logger = Logger.getLogger(SolrCollectionSchemaInfo.class);

    public SolrCollectionSchemaInfo(String collectionName) {
        readSchema(collectionName);
    }

    public void readSchema(String coreName) {
        try {
            String schema = HttpClientUtils.httpGetRequest(SolrUtils.getSolrSchemaURI(coreName));
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(IOUtils.toInputStream(schema));
            doc.getDocumentElement().normalize();

            populateFieldInfo(doc.getElementsByTagName(SCHEMA_FIELD_KEY));
            populateCopyFieldInfo(doc.getElementsByTagName(SCHEMA_COPYFIELD_KEY));

        } catch (Exception e) {
            System.err.println(e.getMessage() + "-- " + e.getLocalizedMessage());
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
                structuredData = schemaField.getDefaultValue().equals("true");
            }

            if (schemaField.isSuggestionCoreField()) {
                suggestionCore = schemaField.getDefaultValue();
            }

            if (SolrUtils.validFieldName(fieldName, true)) {
                facetFieldEntryList.add(fieldName, schemaField.getType(), schemaField.isMultiValued());
            }

            if (!SolrUtils.ignoreFieldName(fieldName)) {
                viewFieldNames.add(fieldName);
            }

            if (schemaField.getType().equals(Constants.SOLR_FIELD_TYPE_DATE)) {
                dateFieldNames.add(fieldName);
            }
        }

        if (fieldInfoMap != null) {
            fieldNames = new ArrayList<String>(fieldInfoMap.keySet());
            Collections.sort(fieldNames);
        }
    }

    public void populateCopyFieldInfo(NodeList copyFields) {
        for(int s = 0; s < copyFields.getLength() ; s++){
            Node field = copyFields.item(s);
            if (field.getNodeType() == Node.ELEMENT_NODE){
                Element el = (Element) field;
                String src = el.getAttribute(COPYFIELD_SOURCE_KEY);
                String dest = el.getAttribute(COPYFIELD_DEST_KEY);

                if (!src.equals(Constants.SOLR_CONTENT_FIELD_NAME) && dest.endsWith(SolrUtils.SOLR_SCHEMA_PREFIXFIELD_ENDSWITH)) {
                    prefixFieldToCopySourceMap.put(dest, el.getAttribute(COPYFIELD_SOURCE_KEY));
                }
            }
        }
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
        return this.suggestionCore != null;
    }

    public String getSuggestionCore() {
        return suggestionCore;
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

    public String getFieldType(String fieldName) {
        return getSchemaField(fieldName).getType();
    }

    public boolean fieldTypeIsDate(String fieldName) {
        SchemaField schemaField = getSchemaField(fieldName);
        return schemaField != null && schemaField.getType().equals(Constants.SOLR_FIELD_TYPE_DATE);
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

    public List<String> getDateFieldNames() {
        return dateFieldNames;
    }

    public List<String> getFacetFieldNames() {
        return facetFieldEntryList.getNames();
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
