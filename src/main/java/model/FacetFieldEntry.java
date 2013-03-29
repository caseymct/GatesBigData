package model;


import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;

public class FacetFieldEntry {
    private String fieldName;
    private String fieldType;
    private boolean multiValued;

    public FacetFieldEntry(String fieldName, String fieldType, boolean multiValued) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.multiValued = multiValued;
    }

    public FacetFieldEntry(String fieldName, String fieldType, String schema) {
        this(fieldName, fieldType, SolrUtils.schemaStringIndicatesMultiValued(schema));
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public boolean fieldTypeIsDate() {
        return fieldType.equals(Constants.SOLR_FIELD_TYPE_DATE);
    }

    public boolean isMultiValued() {
        return this.multiValued;
    }
}
