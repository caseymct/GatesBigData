package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.Defaults;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.Response;
import GatesBigData.utils.JSONUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.FacetFieldEntryList;
import model.SeriesPlot;
import model.SolrCollectionSchemaInfo;
import model.SolrSchemaInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.HDFSService;
import service.SearchService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.*;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/search")
public class SearchAPIController extends APIController {

    public static final String SESSION_HDFSDIR_TOKEN               = "currentHDFSDir";
    public static final String SESSION_VIEWFIELDS_TOKEN            = "viewFields";
    public static final String SESSION_FACETFIELDS_TOKEN           = "facetFields";

    public static int MAX_SUGGESTIONS                               = 15;

    private SearchService searchService;
    private HDFSService hdfsService;
    private SolrSchemaInfo schemaInfo;

    @Autowired
    public SearchAPIController(SearchService searchService, HDFSService hdfsService, SolrSchemaInfo schemaInfo) {
        this.searchService = searchService;
        this.hdfsService   = hdfsService;
        this.schemaInfo    = schemaInfo;
    }

    private FacetFieldEntryList getFacetFields(FacetFieldEntryList allFacetFields, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_FACETFIELDS_TOKEN + coreName;

        FacetFieldEntryList facetFields = (FacetFieldEntryList) session.getAttribute(sessionKey);
        if (Utils.nullOrEmpty(facetFields)) {
            JSONArray facetFieldArray = searchService.getCollectionInfoFieldValuesAsJSONArray(coreName, FieldNames.FACET_FIELDS);

            // If for some reason it's not in Solr, get from HDFS
            if (Utils.nullOrEmpty(facetFieldArray)) {
                String f = hdfsService.getInfoFileFieldContents(coreName, FieldNames.FACET_FIELDS);
                facetFieldArray = JSONUtils.convertStringToJSONArray(f);
            }
            facetFields = SolrUtils.constructFacetFieldEntryList(facetFieldArray, allFacetFields);
            session.setAttribute(sessionKey, facetFields);
        }
        return facetFields;
    }

    private String getViewFields(String schemaViewFieldNames, String collection, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_VIEWFIELDS_TOKEN + collection;

        String viewFields = (String) session.getAttribute(sessionKey);
        if (Utils.nullOrEmpty(viewFields)) {
            viewFields = StringUtils.join(searchService.getViewFields(collection), ",");

            if (Utils.nullOrEmpty(viewFields)) {
                viewFields = schemaViewFieldNames;
            }
            session.setAttribute(sessionKey, viewFields);
        }

        return viewFields;
    }

    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                                @RequestParam(value = PARAM_SORT_FIELD, required = true) String sortField,
                                                @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                                @RequestParam(value = PARAM_START, required = false) Integer start,
                                                @RequestParam(value = PARAM_ROWS, required = false) Integer rows,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);

        FacetFieldEntryList facetFields = getFacetFields(info.getFacetFieldEntryList(), collection, session);
        String viewFields               = getViewFields(info.getViewFieldNamesString(), collection, session);

        searchService.findAndWriteSearchResults(collection, query,
                SolrUtils.getSortClauses(info.getSortFieldIfValid(sortField), sortOrder),
                start, rows, SolrUtils.composeFilterQuery(fq, info),
                facetFields, viewFields, false, info, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/solrfacets", method = RequestMethod.GET)
    public ResponseEntity<String> findFacets(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);

        if (info.hasSuggestionCore()) {
            String suggestionCore = info.getSuggestionCore();
            List<String> fields = schemaInfo.getPrefixTokenFields(suggestionCore);
            searchService.findAndWriteInitialFacetsFromSuggestionCore(suggestionCore, fields, writer);
        } else {
            FacetFieldEntryList facets = getFacetFields(info.getFacetFieldEntryList(), collection, session);
            searchService.findAndWriteFacets(collection, facets, info, writer);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/fields/all", method = RequestMethod.GET)
    public ResponseEntity<String> allFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String collection)
            throws IOException {

        JSONArray ret = new JSONArray();
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        ret.addAll(info.getViewFieldNames());

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/suggest", method = RequestMethod.GET)
    public ResponseEntity<String> suggest(@RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                          @RequestParam(value = PARAM_USER_INPUT, required = true) String userInput,
                                          @RequestParam(value = PARAM_NUM_SUGGESTIONS_PER_FIELD, required = true) int n)
                                        throws IOException {
        JSONArray suggestions;
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        if (info.hasSuggestionCore()) {
            suggestions = searchService.suggestUsingSeparateCore(info.getSuggestionCore(), userInput, MAX_SUGGESTIONS);
        } else {
            suggestions = searchService.suggestUsingGrouping(collection, userInput, info.getPrefixFieldMap(), n);
        }

        JSONObject ret = new JSONObject();
        ret.put(Response.SUGGESTION_KEY, suggestions);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/infofields", method = RequestMethod.GET)
    public ResponseEntity<String> getInfoFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                                @RequestParam(value = PARAM_INFO_FIELD_NAME, required = true) String infoFieldName,
                                                @RequestParam(value = PARAM_GET_NON_NULL_COUNTS, required = false) boolean getNonNullCounts,
                                                @RequestParam(value = PARAM_GET_SEPARATE_FIELD_COUNTS, required = false) boolean getFacetCounts,
                                                @RequestParam(value = PARAM_QUERY, required = false) String queryStr,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        JSONArray fields = searchService.getCollectionInfoFieldValuesAsJSONArray(collection, infoFieldName);
        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        if (getNonNullCounts || getFacetCounts) {
            List<String> listValues = searchService.getFieldCounts(collection, queryStr,
                                                                   SolrUtils.composeFilterQuery(fq, info),
                                                                   JSONUtils.convertJSONArrayToStringList(fields),
                                                                   getFacetCounts);
            fields = JSONUtils.convertCollectionToJSONArray(listValues);
        }

        JSONObject ret = new JSONObject();
        ret.put(infoFieldName.toUpperCase(), fields);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
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

        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);

        SeriesPlot seriesPlot = searchService.getPlotData(collection, query, SolrUtils.composeFilterQuery(fq, info), numFound,
                                        xAxisField,  info.fieldTypeIsDate(xAxisField),
                                        yAxisField,  info.fieldTypeIsDate(yAxisField),
                                        seriesField, info.fieldTypeIsDate(seriesField),
                                        maxPlotPoints, dateRangeGap);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(seriesPlot.getPlotData().toString(), httpHeaders, OK);
    }
}