package GatesBigData.api;

import model.schema.CollectionSchemaInfo;
import model.schema.ReportData;
import model.schema.CollectionsConfig;
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
import java.util.Date;
import java.util.List;
import java.util.Map;

import static GatesBigData.utils.SolrUtils.*;
import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;
import static GatesBigData.constants.Constants.*;
import static GatesBigData.constants.solr.Response.*;

@Controller
@RequestMapping("/report")
public class ReportAPIController extends APIController {

    private SearchService searchService;
    private CollectionsConfig collectionsConfig;

    @Autowired
    public ReportAPIController(SearchService searchService, CollectionsConfig collectionsConfig) {
        this.searchService  = searchService;
        this.collectionsConfig = collectionsConfig;
    }

    @RequestMapping(value="/search", method = RequestMethod.GET)
    public ResponseEntity<String> search(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                         @RequestParam(value = PARAM_QUERY, required = true) String query,
                                         @RequestParam(value = PARAM_SORT, required = false) String sortInfo,
                                         @RequestParam(value = PARAM_START, required = true) Integer start,
                                         @RequestParam(value = PARAM_ROWS, required = true) Integer rows,
                                         @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);
        ReportData data = collectionsConfig.getReportData(collection);

        StringWriter writer = new StringWriter();

        searchService.findAndWriteSearchResults(collection, query, getSortClauses(sortInfo), start, rows,
                                                composeFilterQuery(fq, info), data.getFilterFacetFields(), "",
                                                false, info, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data/dateranges", method = RequestMethod.GET)
    public ResponseEntity<String> getDateRange(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);
        ReportData data = collectionsConfig.getReportData(collection);
        StringWriter writer = new StringWriter();

        JsonGenerator g = searchService.writeSearchResponseStart(writer, null);
        if (data != null) {
            for(String dateField : data.getDateFields()) {
                List<Date> dates = searchService.getSolrFieldDateRange(collection, dateField, info);
                if (dates == null || dates.size() != 2) {
                    continue;
                }

                g.writeObjectFieldStart(dateField);
                g.writeStringField(DATE_RANGE_START_KEY, dates.get(0).toString());
                g.writeStringField(DATE_RANGE_END_KEY,   dates.get(1).toString());
                g.writeEndObject();
            }
        }

        searchService.writeSearchResponseEnd(g);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data", method = RequestMethod.GET)
    public ResponseEntity<String> getDisplayFields(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        StringWriter writer = new StringWriter();

        JsonGenerator g = searchService.writeSearchResponseStart(writer, null);
        collectionsConfig.getReportData(collection).writeData(g);
        searchService.writeSearchResponseEnd(g);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data/metrics", method = RequestMethod.GET)
    public ResponseEntity<String> additionalMetrics(
                                         @RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                         @RequestParam(value = PARAM_QUERY, required = true) String query,
                                         @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        ReportData data = collectionsConfig.getReportData(collection);
        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);

        StringWriter writer = new StringWriter();
        JsonGenerator g = searchService.writeSearchResponseStart(writer, null);

        Map<String, FieldStatsInfo> stats = searchService.getStatsResults(collection, query, composeFilterQuery(fq, info),
                                                                          data.getSearchMetricsViewFields());

        data.writeMetrics(stats, g);
        searchService.writeSearchResponseEnd(g);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/data/filters", method = RequestMethod.GET)
    public ResponseEntity<String> getFilters(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                             @RequestParam(value = PARAM_QUERY, required = true) String queryStr,
                                             @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        ReportData data = collectionsConfig.getReportData(collection);

        StringWriter writer = new StringWriter();
        searchService.findAndWriteFacets(collection, queryStr, fq, data.getFilterFacetFields(),
                collectionsConfig.getSchema(collection), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
     //http://denlx011.dn.gates.com:8983/solr/Inventory_data_collection/select?fl=agg:product(QUANTITY,ACTUAL_COST)&q=*.*&fq=DISTRIBUTION_ACCOUNT_ID:94597&fq=SEGMENT6:%2250%22&fq=PERIOD_NUM:4&wt=json&indent=true