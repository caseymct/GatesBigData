package GatesBigData.api;


import model.schema.CollectionSchemaInfo;
import model.search.FacetFieldEntryList;
import model.analysis.SeriesPlot;
import model.schema.CollectionsConfig;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.HDFSService;
import service.SearchService;

import java.io.*;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;
import static GatesBigData.constants.Constants.*;
import static GatesBigData.utils.JSONUtils.*;
import static GatesBigData.utils.SolrUtils.*;
import static GatesBigData.constants.solr.Response.*;

@Controller
@RequestMapping("/search")
public class SearchAPIController extends APIController {

    public static int MAX_SUGGESTIONS = 15;

    private SearchService searchService;
    private CollectionsConfig collectionsConfig;

    @Autowired
    public SearchAPIController(SearchService searchService, CollectionsConfig collectionsConfig) {
        this.searchService     = searchService;
        this.collectionsConfig = collectionsConfig;
    }

    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                                @RequestParam(value = PARAM_SORT_FIELD, required = true) String sortField,
                                                @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                                @RequestParam(value = PARAM_START, required = false) Integer start,
                                                @RequestParam(value = PARAM_ROWS, required = false) Integer rows,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        StringWriter writer = new StringWriter();
        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);

        FacetFieldEntryList facetFields = collectionsConfig.getFacetFieldEntryList(collection);
        String viewFields               = collectionsConfig.getViewFieldsString(collection);

        searchService.findAndWriteSearchResults(collection, query,
                getSortClauses(info.getSortFieldIfValid(sortField), sortOrder),
                start, rows, composeFilterQuery(fq, info),
                facetFields, viewFields, false, info, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/solrfacets", method = RequestMethod.GET)
    public ResponseEntity<String> findFacets(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        StringWriter writer = new StringWriter();
        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);

        if (info.hasSuggestionCore()) {
            String suggestionCore = info.getSuggestionCore();
            List<String> fields = collectionsConfig.getSchema(suggestionCore).getPrefixTokenFields();
            searchService.findAndWriteInitialFacetsFromSuggestionCore(suggestionCore, fields, writer);
        } else {
            FacetFieldEntryList facets = collectionsConfig.getFacetFieldEntryList(collection);
            searchService.findAndWriteFacets(collection, facets, info, writer);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/fields/all", method = RequestMethod.GET)
    public ResponseEntity<String> allFields(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        JSONArray ret = new JSONArray();
        ret.addAll(collectionsConfig.getSchema(collection).getViewFields());

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/suggest", method = RequestMethod.GET)
    public ResponseEntity<String> suggest(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                          @RequestParam(value = PARAM_USER_INPUT, required = true) String userInput,
                                          @RequestParam(value = PARAM_NUM_SUGGESTIONS_PER_FIELD, required = true) int n)
                                        throws IOException {
        JSONArray suggestions;
        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);
        if (info.hasSuggestionCore()) {
            suggestions = searchService.suggestUsingSeparateCore(info.getSuggestionCore(), userInput, MAX_SUGGESTIONS);
        } else {
            suggestions = searchService.suggestUsingGrouping(collection, userInput, info.getPrefixFieldMap(), n);
        }

        JSONObject ret = new JSONObject();
        ret.put(SUGGESTION_KEY, suggestions);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/infofields", method = RequestMethod.GET)
    public ResponseEntity<String> getInfoFields(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                                @RequestParam(value = PARAM_INFO_FIELD_NAME, required = true) String infoFieldName,
                                                @RequestParam(value = PARAM_GET_NON_NULL_COUNTS, required = false) boolean getNonNullCounts,
                                                @RequestParam(value = PARAM_GET_SEPARATE_FIELD_COUNTS, required = false) boolean getFacetCounts,
                                                @RequestParam(value = PARAM_QUERY, required = false) String queryStr,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        List<String> fields = collectionsConfig.getFieldList(collection, infoFieldName);

        if (getNonNullCounts || getFacetCounts) {
            CollectionSchemaInfo info = collectionsConfig.getSchema(collection);
            fields = searchService.getFieldCounts(collection, queryStr, composeFilterQuery(fq, info), fields, getFacetCounts);
        }

        JSONObject ret = new JSONObject();
        ret.put(infoFieldName, convertCollectionToJSONArray(fields));

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/plot", method = RequestMethod.GET)
    public ResponseEntity<String> getPlotPoints(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                                @RequestParam(value = PARAM_X_AXIS_FIELD, required = true) String xAxisField,
                                                @RequestParam(value = PARAM_Y_AXIS_FIELD, required = true) String yAxisField,
                                                @RequestParam(value = PARAM_SERIES_FIELD, required = false) String seriesField,
                                                @RequestParam(value = PARAM_NUM_FOUND, required = true) Integer numFound,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq,
                                                @RequestParam(value = PARAM_MAX_PLOT_POINTS, required = false) Integer maxPlotPoints,
                                                @RequestParam(value = PARAM_DATE_RANGE_GAP, required = false) String dateRangeGap)
            throws IOException {

        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);

        SeriesPlot seriesPlot = searchService.getPlotData(collection, query, composeFilterQuery(fq, info), numFound,
                                        xAxisField,  info.fieldTypeIsDate(xAxisField),
                                        yAxisField,  info.fieldTypeIsDate(yAxisField),
                                        seriesField, info.fieldTypeIsDate(seriesField),
                                        maxPlotPoints, dateRangeGap);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(seriesPlot.getPlotData().toString(), httpHeaders, OK);
    }
}