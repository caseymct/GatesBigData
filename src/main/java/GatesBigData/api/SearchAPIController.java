package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.FacetFieldEntryList;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.CoreService;
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
    public static final String SESSION_DATEFIELDS_TOKEN            = "dateFields";
    public static final String SESSION_PREFIXTOFULLFIELD_MAP_TOKEN = "prefixToFullFieldMap";

    private SearchService searchService;
    private HDFSService hdfsService;
    private CoreService coreService;

    @Autowired
    public SearchAPIController(SearchService searchService, HDFSService hdfsService, CoreService coreService) {
        this.searchService = searchService;
        this.hdfsService = hdfsService;
        this.coreService = coreService;
    }

    private FacetFieldEntryList getFacetFields(SolrCollectionSchemaInfo schemaInfo, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_FACETFIELDS_TOKEN + coreName;

        FacetFieldEntryList facetFields = (FacetFieldEntryList) session.getAttribute(sessionKey);
        if (Utils.nullOrEmpty(facetFields)) {
            String facetFieldInfo = SolrUtils.getFieldValue(
                    searchService.getSolrDocumentByFieldValue(Constants.SOLR_TITLE_FIELD_NAME, Constants.SOLR_FACET_FIELDS_ID_FIELD_NAME, coreName),
                    Constants.SOLR_FACET_FIELDS_ID_FIELD_NAME, "");

            if (Utils.nullOrEmpty(facetFieldInfo)) {
                facetFieldInfo = hdfsService.getInfoFileContents(coreName, Constants.SOLR_FACET_FIELDS_ID_FIELD_NAME);
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
            viewFields = SolrUtils.getFieldValue(
                    searchService.getSolrDocumentByFieldValue(Constants.SOLR_TITLE_FIELD_NAME, Constants.SOLR_VIEW_FIELDS_ID_FIELD_NAME, coreName),
                    Constants.SOLR_VIEW_FIELDS_ID_FIELD_NAME, "");

            if (Utils.nullOrEmpty(viewFields)) {
                viewFields = hdfsService.getInfoFileContents(coreName, Constants.SOLR_VIEW_FIELDS_ID_FIELD_NAME);
            }
            if (Utils.nullOrEmpty(viewFields)) {
                viewFields = schemaInfo.getViewFieldNamesString();
            }
            session.setAttribute(sessionKey, viewFields);
        }

        return viewFields;
    }

    private List<String> getDateFields(SolrCollectionSchemaInfo schemaInfo, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_DATEFIELDS_TOKEN + coreName;

        List<String> dateFields = (List<String>) session.getAttribute(sessionKey);
        if (dateFields == null) {
            dateFields = schemaInfo.getDateFields();
            session.setAttribute(sessionKey, dateFields);
        }

        return dateFields;
    }

    private Map<String, String> getPrefixToFullFieldMap(SolrCollectionSchemaInfo schemaInfo, String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_PREFIXTOFULLFIELD_MAP_TOKEN + coreName;
        Map<String, String> map = (Map<String, String>) session.getAttribute(sessionKey);

        if (Utils.nullOrEmpty(map)) {
            map = schemaInfo.getPrefixFieldMap();
            session.setAttribute(sessionKey, map);
        }
        return map;
    }

    //http://localhost:8080/GatesBigData/search/solrquery?query=*:*&core=collection1t&sort=score&order=desc&start=0
    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                @RequestParam(value = PARAM_SORT_FIELD, required = true) String sortType,
                                                @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                                @RequestParam(value = PARAM_START, required = false) Integer start,
                                                @RequestParam(value = PARAM_ROWS, required = false) Integer rows,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreService.getSolrServer(coreName), coreName, session);

        searchService.solrSearch(query, coreName, sortType, sortOrder,
                (start == null) ? Constants.SOLR_START_DEFAULT : start,
                (rows == null) ? Constants.SOLR_ROWS_DEFAULT : rows, fq,
                getFacetFields(schemaInfo, coreName, session),
                getViewFields(schemaInfo, coreName, session),
                schemaInfo,
                writer);

        HttpHeaders httpHeaders = new HttpHeaders();

        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/solrfacets", method = RequestMethod.GET)
    public ResponseEntity<String> findFacets(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreService.getSolrServer(coreName),
                coreName, session);

        long currTimeMillis = System.currentTimeMillis();
        FacetFieldEntryList facets = getFacetFields(schemaInfo, coreName, session);

        System.out.println("Time to build list: " + (System.currentTimeMillis() - currTimeMillis));
        System.out.flush();
        currTimeMillis = System.currentTimeMillis();
        searchService.writeFacets(coreName, facets, writer);
        System.out.println("Time to search: " + (System.currentTimeMillis() - currTimeMillis));
        System.out.flush();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/fields/all", method = RequestMethod.GET)
    public ResponseEntity<String> allFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                            HttpServletRequest request) throws IOException {
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreService.getSolrServer(coreName),
                coreName, request.getSession());

        JSONArray ret = new JSONArray();
        ret.addAll(schemaInfo.getViewFieldNames());

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(ret.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/suggest", method = RequestMethod.GET)
    public ResponseEntity<String> suggest(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                          @RequestParam(value = PARAM_USER_INPUT, required = true) String userInput,
                                          @RequestParam(value = PARAM_PREFIX_FIELD, required = false) String prefixField,
                                          @RequestParam(value = PARAM_NUM_SUGGESTIONS_PER_FIELD, required = true) int n,
                                          HttpServletRequest request) throws IOException {

        HttpSession session = request.getSession();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreService.getSolrServer(coreName),
                coreName, session);
        Map<String, String> fieldMap = getPrefixToFullFieldMap(schemaInfo, coreName, session);

        if (prefixField != null) {
            Map<String, String> singleMap = new HashMap<String, String>();
            singleMap.put(prefixField, fieldMap.get(prefixField));
            fieldMap = singleMap;
        }

        JSONObject suggest = searchService.suggest(coreName, userInput, fieldMap, n);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(suggest.toString(), httpHeaders, OK);
    }
}
