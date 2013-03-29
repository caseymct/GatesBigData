package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.JsonParsingUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.FacetFieldEntryList;
import model.SeriesPlot;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.HDFSService;
import service.SearchService;
import service.SolrService;

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
    public static final String SESSION_SUGGESTION_CORE_NAME_TOKEN  = "suggestionCoreName";

    public static int MAX_SUGGESTIONS                               = 15;

    private static final String NO_CORRESPONDING_SUGGESTION_CORE   = "none";

    private SearchService searchService;
    private HDFSService hdfsService;
    private SolrService solrService;

    @Autowired
    public SearchAPIController(SearchService searchService, HDFSService hdfsService, SolrService solrService) {
        this.searchService = searchService;
        this.hdfsService = hdfsService;
        this.solrService = solrService;
    }

    private FacetFieldEntryList getFacetFields(SolrCollectionSchemaInfo schemaInfo, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_FACETFIELDS_TOKEN + coreName;

        FacetFieldEntryList facetFields = (FacetFieldEntryList) session.getAttribute(sessionKey);
        if (Utils.nullOrEmpty(facetFields)) {
            String facetFieldInfo = searchService.getCoreInfoFieldValue(coreName, Constants.SOLR_FACET_FIELDS_ID_FIELD_NAME);

            if (Utils.nullOrEmpty(facetFieldInfo)) {
                facetFieldInfo = hdfsService.getInfoFilesContents(coreName).get(Constants.SOLR_FACET_FIELDS_ID_FIELD_NAME);
            }
            facetFields = SolrUtils.constructFacetFieldEntryList(facetFieldInfo, schemaInfo);
            session.setAttribute(sessionKey, facetFields);
        }
        return facetFields;
    }

    private String getViewFields(SolrCollectionSchemaInfo schemaInfo, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_VIEWFIELDS_TOKEN + coreName;

        String viewFields = (String) session.getAttribute(sessionKey);
        if (Utils.nullOrEmpty(viewFields)) {
            Set<String> fieldList = new HashSet<String>();
            fieldList.addAll(SolrUtils.getFieldsFromInfoString(searchService.getCoreInfoFieldValue(coreName, Constants.SOLR_PREVIEW_FIELDS_ID_FIELD_NAME)));
            fieldList.addAll(Arrays.asList(Constants.SOLR_ID_FIELD_NAME, Constants.SOLR_URL_FIELD_NAME));
            viewFields = StringUtils.join(fieldList, ",");

            if (Utils.nullOrEmpty(viewFields)) {
                viewFields = schemaInfo.getViewFieldNamesString();
            }
            session.setAttribute(sessionKey, viewFields);
        }

        return viewFields;
    }

    private String getSuggestionCoreName(String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_SUGGESTION_CORE_NAME_TOKEN;

        List<String> suggestionCoreNames = (List<String>) session.getAttribute(sessionKey);
        String suggestionCoreName = SolrUtils.getSuggestionCoreName(coreName);
        if (suggestionCoreNames == null) {

            // or maybe search in a solr field here
            suggestionCoreNames = solrService.getCoreNames();
            session.setAttribute(sessionKey, suggestionCoreNames);
        }

        return suggestionCoreNames.contains(suggestionCoreName) ? suggestionCoreName : NO_CORRESPONDING_SUGGESTION_CORE;
    }

    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                @RequestParam(value = PARAM_SORT_FIELD, required = true) String sortField,
                                                @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                                @RequestParam(value = PARAM_START, required = false) Integer start,
                                                @RequestParam(value = PARAM_ROWS, required = false) Integer rows,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, session);

        if (schemaInfo.isFieldMultiValued(sortField)) {
            sortField = Constants.SOLR_SORT_FIELD_DEFAULT;
        }

        searchService.solrSearch(query, coreName, sortField, sortOrder,
                (start == null) ? Constants.SOLR_START_DEFAULT : start,
                (rows == null)  ? Constants.SOLR_ROWS_DEFAULT  : rows, fq,
                getFacetFields(schemaInfo, coreName, session), getViewFields(schemaInfo, coreName, session),
                schemaInfo.getDateFieldNames(), writer);

        HttpHeaders httpHeaders = new HttpHeaders();

        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/solrfacets", method = RequestMethod.GET)
    public ResponseEntity<String> findFacets(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, session);

        FacetFieldEntryList facets = getFacetFields(schemaInfo, coreName, session);
        searchService.writeFacets(coreName, facets, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/fields/all", method = RequestMethod.GET)
    public ResponseEntity<String> allFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                            HttpServletRequest request) throws IOException {
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, request.getSession());

        JSONArray ret = new JSONArray();
        ret.addAll(schemaInfo.getViewFieldNames());

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/suggest", method = RequestMethod.GET)
    public ResponseEntity<String> suggest(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                          @RequestParam(value = PARAM_USER_INPUT, required = true) String userInput,
                                          @RequestParam(value = PARAM_NUM_SUGGESTIONS_PER_FIELD, required = true) int n,
                                          HttpServletRequest request) throws IOException {

        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo info = getSolrCollectionSchemaInfo(coreName, session);
        //String suggestionCoreName = getSuggestionCoreName(coreName, session);

        JSONArray suggestions;
        if (info.hasSuggestionCore()) {
            suggestions = searchService.suggestUsingSeparateCore(info.getSuggestionCore(), userInput, MAX_SUGGESTIONS);
        } else {
            suggestions = searchService.suggestUsingGrouping(coreName, userInput, info.getPrefixFieldMap(), n);
        }

        JSONObject ret = new JSONObject();
        ret.put(Constants.SUGGESTION_RESPONSE_KEY, suggestions);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/infofields", method = RequestMethod.GET)
    public ResponseEntity<String> getInfoFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                @RequestParam(value = PARAM_INFO_FIELD_TYPE, required = true) String infoFieldName,
                                                @RequestParam(value = PARAM_GET_NON_NULL_COUNTS, required = false) boolean getNonNullCounts,
                                                @RequestParam(value = PARAM_GET_SEPARATE_FIELD_COUNTS, required = false) boolean getFacetCounts,
                                                @RequestParam(value = PARAM_QUERY, required = false) String queryStr,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        String fieldString = searchService.getCoreInfoFieldValue(coreName, infoFieldName);

        List<String> fields = Arrays.asList(StringUtils.split(fieldString, ","));
        List<String> listValues = fields;
        if (getNonNullCounts || getFacetCounts) {
            listValues = searchService.getFieldCounts(coreName, queryStr, fq, fields, getFacetCounts);
        }

        JSONObject ret = new JSONObject();
        ret.put(infoFieldName, JsonParsingUtils.convertStringListToJSONArray(listValues));

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/plot", method = RequestMethod.GET)
    public ResponseEntity<String> getPlotPoints(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                @RequestParam(value = PARAM_X_AXIS_FIELD, required = true) String xAxisField,
                                                @RequestParam(value = PARAM_Y_AXIS_FIELD, required = true) String yAxisField,
                                                @RequestParam(value = PARAM_SERIES_FIELD, required = false) String seriesField,
                                                @RequestParam(value = PARAM_NUM_FOUND, required = true) Integer numFound,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, session);

        SeriesPlot seriesPlot = searchService.getPlotData(coreName, query, fq, numFound, xAxisField,
                schemaInfo.fieldTypeIsDate(xAxisField), yAxisField, schemaInfo.fieldTypeIsDate(yAxisField),
                seriesField, schemaInfo.fieldTypeIsDate(seriesField));

        writer.append(seriesPlot.getPlotData().toString());

        HttpHeaders httpHeaders = new HttpHeaders();

        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}