package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.utils.JSONUtils;
import GatesBigData.utils.SolrUtils;
import model.*;
import model.reports.ReportConstants.Filters;
import model.reports.CollectionReportData;
import model.reports.ReportData;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.SearchService;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/report")
public class ReportAPIController extends APIController {

    private SearchService searchService;
    private ReportData reportData;
    private SolrSchemaInfo schemaInfo;

    @Autowired
    public ReportAPIController(SearchService searchService, SolrSchemaInfo schemaInfo) {
        this.searchService  = searchService;
        this.schemaInfo     = schemaInfo;
        this.reportData     = new ReportData();
    }

    private CollectionReportData getCollectionReportData(String collection) {
        if (!reportData.hasCollectionData(collection)) {
            String reportDataStr    = searchService.getCollectionInfoFieldValue(collection, FieldNames.REPORT_DATA_FIELDS);
            String displayNamesStr  = searchService.getCollectionInfoFieldValue(collection, FieldNames.DISPLAY_NAMES_FIELDS);
            JSONObject reportJSON   = JSONUtils.convertStringToJSONObject(reportDataStr);
            JSONArray displayNames  = JSONUtils.convertStringToJSONArray(displayNamesStr);

            if (reportJSON != null) {
                this.reportData.add(collection, reportJSON, displayNames, schemaInfo.getViewFieldNames(collection),
                        schemaInfo.getFacetFieldEntryList(collection));
            }
        }
        return this.reportData.getCollectionReportData(collection);
    }

    @RequestMapping(value="/search", method = RequestMethod.GET)
    public ResponseEntity<String> search(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                         @RequestParam(value = PARAM_QUERY, required = true) String query,
                                         @RequestParam(value = PARAM_SORT, required = false) String sortInfo,
                                         @RequestParam(value = PARAM_START, required = true) Integer start,
                                         @RequestParam(value = PARAM_ROWS, required = true) Integer rows,
                                         @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        CollectionReportData data = getCollectionReportData(collection);
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        StringWriter writer = new StringWriter();

        searchService.findAndWriteSearchResults(collection, query, SolrUtils.getSortClauses(sortInfo),
                start, rows, SolrUtils.composeFilterQuery(fq, info),
                data.getFilterFacetFields(), "", false, info, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data/dateranges", method = RequestMethod.GET)
    public ResponseEntity<String> getDateRange(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        CollectionReportData data = getCollectionReportData(collection);
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        StringWriter writer = new StringWriter();

        JsonGenerator g = searchService.writeSearchResponseStart(writer, null);
        if (data != null) {
            for(String dateField : data.getDateFilters()) {
                List<Date> dates = searchService.getSolrFieldDateRange(collection, dateField, info);

                g.writeObjectFieldStart(dateField);
                if (dates != null && dates.size() == 2) {
                    g.writeStringField(Filters.DATE_FILTERS_RANGE_START_KEY, dates.get(0).toString());
                    g.writeStringField(Filters.DATE_FILTERS_RANGE_END_KEY,   dates.get(1).toString());
                }
                g.writeEndObject();
            }
        }

        searchService.writeSearchResponseEnd(g);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data", method = RequestMethod.GET)
    public ResponseEntity<String> getDisplayFields(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        CollectionReportData data = getCollectionReportData(collection);
        StringWriter writer = new StringWriter();

        JsonGenerator g = searchService.writeSearchResponseStart(writer, null);
        data.writeData(g);
        searchService.writeSearchResponseEnd(g);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data/metrics", method = RequestMethod.GET)
    public ResponseEntity<String> additionalMetrics(
                                         @RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                         @RequestParam(value = PARAM_QUERY, required = true) String query,
                                         @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        CollectionReportData data = getCollectionReportData(collection);
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        StringWriter writer = new StringWriter();
        JsonGenerator g = searchService.writeSearchResponseStart(writer, null);

        Map<String, FieldStatsInfo> stats = searchService.getStatsResults(collection, query,
                SolrUtils.composeFilterQuery(fq, info),
                data.getSearchMetricsViewFields());

        data.writeMetrics(stats, g);
        searchService.writeSearchResponseEnd(g);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data/filters", method = RequestMethod.GET)
    public ResponseEntity<String> getFilters(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                             @RequestParam(value = PARAM_QUERY, required = true) String queryStr,
                                             @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        CollectionReportData data = getCollectionReportData(collection);
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        StringWriter writer = new StringWriter();
        searchService.findAndWriteFacets(collection, queryStr, fq, data.getFilterFacetFields(), info, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
     //http://denlx011.dn.gates.com:8983/solr/Inventory_data_collection/select?fl=agg:product(QUANTITY,ACTUAL_COST)&q=*.*&fq=DISTRIBUTION_ACCOUNT_ID:94597&fq=SEGMENT6:%2250%22&fq=PERIOD_NUM:4&wt=json&indent=true