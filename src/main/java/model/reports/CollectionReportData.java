package model.reports;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.utils.JSONUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.FacetFieldEntryList;
import model.reports.ReportConstants.Filters;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.*;

/*
       "REPORT_DATA"={
   title:"Material Distributions Report",
           filters:["PERIOD_NAME","SEGMENT1","SEGMENT2","SEGMENT3","SEGMENT4","ORG_CODE","TXN_TYPE_NAME"],
   datefilters:["TXN_DATE"],
   sortfilters:["TXN_DATE","ORG_CODE","TRANSACTION_VALUE"],
   display:["TXN_DATE","TXN_TYPE_NAME","ITEM_DESCRIPTION","ORG_CODE","UOM_CODE","SUBINV_CODE","PRODUCT_NUMBER",
           "PERIOD_NAME","QUANTITY","ACTUAL_COST","TRANSACTION_VALUE","SOURCE_TYPE_NAME","CONCAT_CCD_CODE"],
   aggregate:[{name:"TOTAL_COST", docfield:"TOTAL_COST"},{name:"TOTAL_QUANTITY",docfield:"PRIMARY_QUANTITY"}]
}]
*/

public class CollectionReportData {
    private JSONObject reportData;
    private String title;
    private List<String> filters                            = new ArrayList<String>();
    private List<String> dateFilters                        = new ArrayList<String>();
    private List<String> sortFilters                        = new ArrayList<String>();
    private List<String> fieldsToDisplay                    = new ArrayList<String>();
    private List<String> exportFields                       = new ArrayList<String>();
    private FacetFieldEntryList filterFacetFields           = new FacetFieldEntryList();
    private SearchResultMetricsList searchResultMetricsList;
    private JSONArray displayNames;

    public CollectionReportData(JSONObject reportData, JSONArray displayNames) {
        this.reportData     = reportData;
        this.displayNames   = displayNames;

        initialize();
    }

    public CollectionReportData(JSONObject reportData, JSONArray displayNames, List<String> viewFields, FacetFieldEntryList allFacets) {
        this(reportData, displayNames);

        initializeFacetFields(allFacets);
        initializeExportFields(viewFields);
    }

    private void initialize() {
        title           = JSONUtils.getStringValue(this.reportData, ReportConstants.REPORT_TITLE_KEY);
        filters         = ReportConstants.Filters.getFilters(this.reportData);
        dateFilters     = ReportConstants.Filters.getDateFilters(this.reportData);
        sortFilters     = ReportConstants.Filters.getSortFilters(this.reportData);
        fieldsToDisplay = ReportConstants.getFieldsToDisplay(this.reportData);

        JSONArray aggregate = JSONUtils.getJSONArrayValue(this.reportData, ReportConstants.AggregateMetrics.AGGREGATE_KEY);
        searchResultMetricsList = new SearchResultMetricsList(aggregate);
    }

    private void initializeFacetFields(FacetFieldEntryList allFacets) {
        if (allFacets != null) {
            filterFacetFields = SolrUtils.constructFacetFieldEntryList(filters, allFacets);
        }
    }

    public void initializeExportFields(List<String> viewFields) {
        exportFields = new ArrayList<String>();
        if (viewFields != null) {
            for(String viewField : viewFields) {
                if (!viewField.endsWith("_ID")) {
                    exportFields.add(viewField);
                }
            }
        }
    }

    public String getTitle() {
        return this.title;
    }

    public FacetFieldEntryList getFilterFacetFields() {
        return this.filterFacetFields;
    }

    public List<String> getDateFilters() {
        return dateFilters;
    }

    public void writeMetrics(Map<String, FieldStatsInfo> infoMap, JsonGenerator g) throws IOException {
        searchResultMetricsList.writeMetrics(infoMap, g);
    }

    public List<String> getSearchMetricsViewFields() {
        return searchResultMetricsList.getViewFields();
    }

    public void writeData(JsonGenerator g) throws IOException {
        g.writeStringField(FieldNames.TITLE, this.title);
        Utils.writeJSONArray(Filters.FILTERS_KEY, this.filters, g);
        Utils.writeJSONArray(Filters.SORT_FILTERS_KEY, this.sortFilters, g);
        Utils.writeJSONArray(Filters.DATE_FILTERS_KEY, this.dateFilters, g);
        Utils.writeJSONArray(ReportConstants.FIELDS_TO_EXPORT_KEY, this.exportFields, g);
        Utils.writeJSONArray(ReportConstants.REPORT_DISPLAY_KEY, this.fieldsToDisplay, g);
        Utils.writeValueByType(ReportConstants.NAMES_TO_DISPLAY_NAMES_KEY, this.displayNames, g);
    }
}
