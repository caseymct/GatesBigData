package model;


public class FacetFieldEntry {
    private String fieldName;
    private String fieldType;
    private boolean multiValued;

    public FacetFieldEntry(String fieldName, String fieldType, String schema) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.multiValued = (schema.charAt(3) == 77);
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

    public boolean isMultiValued() {
        return this.multiValued;
    }


}
