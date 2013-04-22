package model;

import GatesBigData.utils.Constants;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class SchemaField {
    public static final String NAME_KEY              = "name";
    public static final String TYPE_KEY              = "type";
    public static final String DEFAULT_VALUE_KEY     = "default";
    public static final String INDEXED_VALUE_KEY     = "indexed";
    public static final String STORED_VALUE_KEY      = "stored";
    public static final String MULTIVALUED_VALUE_KEY = "multiValued";
    public static final String REQUIRED_VALUE_KEY    = "required";

    public String name;
    public String type;
    public String defaultValue = null;
    public boolean indexed = true;
    public boolean multiValued = false;
    public boolean stored;
    public boolean required = false;

    SchemaField(NamedNodeMap map) {
        this.name         = getNodeValue(map, NAME_KEY);
        this.type         = getNodeValue(map, TYPE_KEY);
        this.defaultValue = getNodeValue(map, DEFAULT_VALUE_KEY);
        this.indexed      = getNodeBooleanValue(map, INDEXED_VALUE_KEY);
        this.stored       = getNodeBooleanValue(map, STORED_VALUE_KEY);
        this.multiValued  = getNodeBooleanValue(map, MULTIVALUED_VALUE_KEY);
        this.required     = getNodeBooleanValue(map, REQUIRED_VALUE_KEY);
    }

    SchemaField(String name, String type, boolean stored, boolean indexed) {
        this.name = name;
        this.type = type;
        this.stored = stored;
        this.indexed = indexed;
    }

    SchemaField(String name, String type, String defaultValue, boolean stored, boolean indexed) {
        this(name, type, stored, indexed);
        this.defaultValue = defaultValue;
    }

    private boolean getNodeBooleanValue(NamedNodeMap map, String name) {
        return getNodeValue(map, name).equals("true");
    }

    private String getNodeValue(NamedNodeMap map, String name) {
        Node node = map.getNamedItem(name);
        if (node == null) return "";
        return node.getNodeValue();
    }

    public boolean isCoreTitleField() {
        return this.name.equals(Constants.SOLR_FIELD_NAME_CORE_TITLE);
    }

    public boolean isSuggestionCoreField() {
        return this.name.equals(Constants.SOLR_FIELD_NAME_SUGGESTION_CORE);
    }

    public boolean isStructuredDataField() {
        return this.name.equals(Constants.SOLR_FIELD_NAME_STRUCTURED_DATA);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    public boolean isMultiValued() {
        return multiValued;
    }

    public void setMultiValued(boolean multiValued) {
        this.multiValued = multiValued;
    }

    public boolean isStored() {
        return stored;
    }

    public void setStored(boolean stored) {
        this.stored = stored;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }
}
