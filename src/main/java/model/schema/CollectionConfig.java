package model.schema;

import model.search.FacetFieldEntryList;
import org.w3c.dom.NodeList;

import java.util.*;

import static GatesBigData.constants.XmlConfig.*;
import static GatesBigData.utils.SolrUtils.constructFacetFieldEntryList;
import static GatesBigData.utils.Utils.*;
import static GatesBigData.constants.solr.FieldNames.*;

public class CollectionConfig {
    private HashMap<String, String> properties = new HashMap<String, String>();

    private String collection = "";

    private HashMap<String, List<String>> collectionFieldsMap = new HashMap<String, List<String>>();
    private ReportData reportData;

    private List<String> viewFields;
    private FacetFieldEntryList facetFields;

    public CollectionConfig(NodeList nodes) {
        initialize(nodes);
        initializeViewFields();
    }

    private void initialize(NodeList nodes) {
        this.reportData = new ReportData();

        for(XmlProperty xmlProperty : getXmlProperties(nodes)) {
            if (isReportTag(xmlProperty.getName())) {
                this.reportData.setProperty(xmlProperty);
            } else if (isFieldsTag(xmlProperty.getName())) {
                this.collectionFieldsMap.put(xmlProperty.getName(), xmlProperty.getValueList());
            } else if (xmlProperty.hasNameAndValue()) {
                this.properties.put(xmlProperty.getName(), xmlProperty.getValue());
            }
        }

        this.collection = this.properties.get(PROPERTY_COLLECTION_NAME);
    }

    private void initializeViewFields() {
        Set<String> viewFieldsSet = new HashSet<String>();
        viewFieldsSet.addAll(this.collectionFieldsMap.get(PROPERTY_FIELDS_PREVIEW));
        viewFieldsSet.addAll(this.collectionFieldsMap.get(PROPERTY_FIELDS_TABLE));
        viewFieldsSet.addAll(Arrays.asList(ID, URL));

        this.viewFields = new ArrayList<String>(viewFieldsSet);
        Collections.sort(this.viewFields);
    }

    public void initializeFacetFieldEntryList(FacetFieldEntryList allFacets) {
        if (this.reportData != null) {
            this.reportData.initializeFacetFields(allFacets);
        }

        this.facetFields = constructFacetFieldEntryList(getFacetFields(), allFacets);
    }

    public ReportData getReportData() {
        return this.reportData;
    }

    public String getCollection() {
        return this.collection;
    }

    public List<String> getViewFields() {
        return this.viewFields;
    }

    public List<String> getPreviewFields() {
        return getFieldList(PROPERTY_FIELDS_PREVIEW);
    }

    public List<String> getWordTreeFields() {
        return getFieldList(PROPERTY_FIELDS_WORDTREE);
    }

    public List<String> getFieldList(String key) {
        return this.collectionFieldsMap.containsKey(key) ? this.collectionFieldsMap.get(key) : new ArrayList<String>();
    }

    public List<String> getFacetFields() {
        return this.collectionFieldsMap.get(PROPERTY_FIELDS_FACET);
    }

    public FacetFieldEntryList getFacetFieldEntryList() {
        return this.facetFields;
    }
}
