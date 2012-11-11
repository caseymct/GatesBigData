package LucidWorksApp.api;

import LucidWorksApp.utils.SolrUtils;
import model.FacetFieldEntryList;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;


@Controller
@RequestMapping("/search")
public class SearchAPIController extends APIController {

    public static final String SESSION_HDFSDIR_TOKEN               = "currentHDFSDir";
    public static final String SESSION_PREFIXTOFULLFIELD_MAP_TOKEN = "prefixToFullFieldMap";

    private SearchService searchService;
    private HDFSService hdfsService;

    @Autowired
    public SearchAPIController(SearchService searchService, HDFSService hdfsService) {
        this.searchService = searchService;
        this.hdfsService = hdfsService;
    }

    private FacetFieldEntryList getFacetFields(String coreName, HttpSession session) {
        String hdfsDir = hdfsService.getHDFSCoreDirectory(false, coreName).toString();

        FacetFieldEntryList hdfsFacetFields = (FacetFieldEntryList) session.getAttribute(SESSION_HDFSDIR_TOKEN + hdfsDir);
        if (hdfsFacetFields == null || hdfsFacetFields.size() == 0) {
            hdfsFacetFields = hdfsService.getHDFSFacetFields(coreName);

            FacetFieldEntryList newFacetFields = new FacetFieldEntryList();
            List<String> lukeFacetFieldNames = SolrUtils.getLukeFacetFieldNames(coreName);
            for(String name: hdfsFacetFields.getNames()) {
                if (lukeFacetFieldNames.contains(name)) {
                    newFacetFields.add(hdfsFacetFields.get(name));
                }
            }
            session.setAttribute(SESSION_HDFSDIR_TOKEN + hdfsDir, newFacetFields);
            return newFacetFields;
        }
        return hdfsFacetFields;
    }


    private HashMap<String, String> getPrefixToFullFieldMap(String coreName, HttpSession session) {
        String fullToken = coreName + "_" + SESSION_PREFIXTOFULLFIELD_MAP_TOKEN;
        HashMap<String, String> map = (HashMap<String, String>) session.getAttribute(fullToken);

        if (map == null || map.size() == 0) {
            map = SolrUtils.getPrefixFieldMap(coreName);
            session.setAttribute(fullToken, map);
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

        searchService.solrSearch(query, coreName, sortType, sortOrder, (start == null) ? 0 : start,
                (rows == null) ? 10 : rows, fq, facetFields, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/solrfacets", method = RequestMethod.GET)
    public ResponseEntity<String> findFacets(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                HttpServletRequest request) throws IOException {

        FacetFieldEntryList facetFields = getFacetFields(coreName, request.getSession());
        StringWriter writer = new StringWriter();
        searchService.writeFacets(coreName, facetFields, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
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
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(suggest.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/suggest/all", method = RequestMethod.GET)
    public ResponseEntity<String> suggest(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                          @RequestParam(value = PARAM_USER_INPUT, required = true) String userInput,
                                          @RequestParam(value = PARAM_NUM_SUGGESTIONS_PER_FIELD, required = true) int n,
                                          HttpServletRequest request) throws IOException {

        HashMap<String, String> fieldMap = getPrefixToFullFieldMap(coreName, request.getSession());
        JSONObject suggest = searchService.suggest(coreName, userInput, fieldMap, n);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(suggest.toString(), httpHeaders, OK);
    }
}
