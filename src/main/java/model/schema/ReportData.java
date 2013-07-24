package model.schema;

import model.search.FacetFieldEntryList;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.codehaus.jackson.JsonGenerator;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.util.*;

import static GatesBigData.constants.XmlConfig.isTag;
import static GatesBigData.utils.Utils.*;
import static GatesBigData.utils.SolrUtils.*;
import static GatesBigData.constants.XmlConfig.*;

public class ReportData {

    private List<String> exportFields                       = new ArrayList<String>();
    private FacetFieldEntryList filterFacetFields           = new FacetFieldEntryList();
    private HashMap<String, String> aggregateFields       = new HashMap<String, String>();
    private HashMap<String, List<String>> fieldsMap       = new HashMap<String, List<String>>();
    private String title;

    public ReportData() { }

    public boolean hasReportData() {
        return !nullOrEmpty(fieldsMap) && !nullOrEmpty(title);
    }

    public void setProperty(XmlProperty property) {
        String tag = property.getName();

        if (isReportAggregateFieldTag(tag)) {
            setAggregateFields(property.getChildProperties());

        } else if (isReportFieldsTag(tag)) {
            List<String> fields = new ArrayList<String>();
            for(String field : getTokens(property.getValue(), ",")) {
                fields.add(field.trim());
            }
            Collections.sort(fields);
            this.fieldsMap.put(tag, fields);

            if (isTag(tag, PROPERTY_REPORT_DISPLAY_FIELDS)) {
                initializeExportFields();
            }

        } else if (isTag(tag, PROPERTY_REPORT_TITLE)) {
            this.title = property.getValue();
        }
    }

    public void setAggregateFields(List<XmlProperty> xmlProperties) {
        String displayName = null, solrField = null;
        for(XmlProperty xmlProperty : xmlProperties) {
            if (isTag(xmlProperty.getName(), PROPERTY_REPORT_AGGREGATE_FIELD_DISPLAY)) {
                displayName = xmlProperty.getValue();
            } else if (isTag(xmlProperty.getName(), PROPERTY_REPORT_AGGREGATE_FIELD_SOLRFIELD)) {
                solrField = xmlProperty.getValue();
            }
        }
        this.aggregateFields.put(displayName, solrField);
    }

    public void initializeFacetFields(FacetFieldEntryList allFacets) {
        this.filterFacetFields = constructFacetFieldEntryList(
                this.fieldsMap.get(PROPERTY_REPORT_FILTERS_FIELDS), allFacets);
    }

    public void initializeExportFields() {        
        this.exportFields = new ArrayList<String>();
        for(String viewField : this.fieldsMap.get(PROPERTY_REPORT_DISPLAY_FIELDS)) {
            if (!viewField.endsWith("_ID")) {
                exportFields.add(viewField);
            }
        }
    }

    public FacetFieldEntryList getFilterFacetFields() {
        return this.filterFacetFields;
    }

    public List<String> getDateFields() {
        return this.fieldsMap.get(PROPERTY_REPORT_FILTERS_DATEFIELDS);
    }

    public void writeMetrics(Map<String, FieldStatsInfo> infoMap, JsonGenerator g) throws IOException {

        for(Map.Entry<String, FieldStatsInfo> entry : infoMap.entrySet()) {    // e.g. "TOTAL_QUANTITY"
            String displayFieldName = entry.getKey();
            FieldStatsInfo info     = entry.getValue();

            g.writeArrayFieldStart(displayFieldName);
            g.writeStartObject();
            writeValueByType("name", aggregateFields.get(displayFieldName), g);
            writeValueByType("min", info.getMin(), g);
            writeValueByType("max", info.getMax(), g);
            writeValueByType("count", info.getCount(), g);
            writeValueByType("missing", info.getMissing(), g);
            writeValueByType("sum", info.getSum(), g);
            writeValueByType("mean", info.getMean(), g);
            writeValueByType("stddev", info.getStddev(), g);
            g.writeEndObject();
            g.writeEndArray();
        }
    }

    public List<String> getSearchMetricsViewFields() {
        return new ArrayList<String>(aggregateFields.values());
    }

    public void writeData(JsonGenerator g) throws IOException {
        writeValueByType(PROPERTY_REPORT_TITLE, this.title, g);
        writeJSONArray(PROPERTY_REPORT_EXPORT_FIELDS, this.exportFields, g);

        for(String p : Arrays.asList(PROPERTY_REPORT_FILTERS_FIELDS, PROPERTY_REPORT_FILTERS_SORTFIELDS,
                                     PROPERTY_REPORT_FILTERS_DATEFIELDS, PROPERTY_REPORT_DISPLAY_FIELDS)) {
            writeJSONArray(p, this.fieldsMap.get(p), g);
        }
    }
}
