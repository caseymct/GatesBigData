package LucidWorksApp.api;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.SolrUtils;
import LucidWorksApp.utils.Utils;
import model.FacetFieldEntryList;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    @Autowired
    public SearchAPIController(SearchService searchService, HDFSService hdfsService) {
        this.searchService = searchService;
        this.hdfsService = hdfsService;
    }

    private FacetFieldEntryList getFacetFields(String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_FACETFIELDS_TOKEN + coreName;

        FacetFieldEntryList hdfsFacetFields = (FacetFieldEntryList) session.getAttribute(sessionKey);
        if (hdfsFacetFields == null || hdfsFacetFields.size() == 0) {
            hdfsFacetFields = hdfsService.getHDFSFacetFields(coreName);

            FacetFieldEntryList newFacetFields = new FacetFieldEntryList();
            List<String> lukeFacetFieldNames = SolrUtils.getLukeFacetFieldNames(coreName);
            for(String name: hdfsFacetFields.getNames()) {
                if (lukeFacetFieldNames.contains(name)) {
                    newFacetFields.add(hdfsFacetFields.get(name));
                }
            }
            session.setAttribute(sessionKey, newFacetFields);
            return newFacetFields;
        }
        return hdfsFacetFields;
    }

    private String getViewFields(String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_VIEWFIELDS_TOKEN + coreName;

        String viewFields = (String) session.getAttribute(sessionKey);
        if (viewFields == null || viewFields.equals("")) {
            viewFields = hdfsService.getHDFSViewFields(coreName);
            session.setAttribute(sessionKey, viewFields);
        }

        return viewFields;
    }

    private List<String> getDateFields(String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_DATEFIELDS_TOKEN + coreName;

        List<String> dateFields = (List<String>) session.getAttribute(sessionKey);
        if (dateFields == null) {
            dateFields = SolrUtils.getLukeFieldNamesByType(coreName, "date");
            session.setAttribute(sessionKey, dateFields);
        }

        return dateFields;
    }

    private HashMap<String, String> getPrefixToFullFieldMap(String coreName, HttpSession session) {
        String sessionKey = SESSION_HDFSDIR_TOKEN + SESSION_PREFIXTOFULLFIELD_MAP_TOKEN + coreName;
        HashMap<String, String> map = (HashMap<String, String>) session.getAttribute(sessionKey);

        if (map == null || map.size() == 0) {
            map = SolrUtils.getPrefixFieldMap(coreName);
            session.setAttribute(sessionKey, map);
        }
        return map;
    }

    //http://localhost:8080/LucidWorksApp/search/solrquery?query=*:*&core=collection1t&sort=score&order=desc&start=0
    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                                @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                @RequestParam(value = PARAM_SORT_TYPE, required = true) String sortType,
                                                @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                                @RequestParam(value = PARAM_START, required = false) Integer start,
                                                @RequestParam(value = PARAM_ROWS, required = false) Integer rows,
                                                @RequestParam(value = PARAM_FQ, required = false) String fq,
                                                HttpServletRequest request) throws IOException {

        StringWriter writer = new StringWriter();
        HttpSession session = request.getSession();
        FacetFieldEntryList facetFields = getFacetFields(coreName, session);
        String viewFields               = getViewFields(coreName, session);
        List<String> dateViewFields     = getDateFields(coreName, session);

        searchService.solrSearch(query, coreName, sortType, sortOrder,
                (start == null) ? Constants.SOLR_START_DEFAULT : start,
                (rows == null) ? Constants.SOLR_ROWS_DEFAULT : rows,
                fq, facetFields, viewFields, dateViewFields, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/solrfacets", method = RequestMethod.GET)
    public ResponseEntity<String> findFacets(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                HttpServletRequest request) throws IOException {

        FacetFieldEntryList facetFields = getFacetFields(coreName, request.getSession());
        StringWriter writer = new StringWriter();
        searchService.writeFacets(coreName, facetFields, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/fields/all", method = RequestMethod.GET)
    public ResponseEntity<String> allFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) throws IOException {
        JSONArray ret = new JSONArray();
        ret.addAll(SolrUtils.getViewFieldNames(coreName));

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

        HashMap<String, String> fieldMap = getPrefixToFullFieldMap(coreName, request.getSession());
        HashMap<String, String> singleMap = new HashMap<String, String>();
        if (prefixField != null) {
            singleMap.put(prefixField, fieldMap.get(prefixField));
        }

        JSONObject suggest = searchService.suggest(coreName, userInput, (prefixField != null) ? singleMap : fieldMap, n);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(suggest.toString(), httpHeaders, OK);
    }
}
