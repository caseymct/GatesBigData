package LucidWorksApp.api;

import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;
import model.FacetFieldEntry;
import model.FacetFieldEntryList;
import net.sf.json.JSONObject;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
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
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;


@Controller
@RequestMapping("/search")
public class SearchAPIController extends APIController {

    public static final String SESSION_HDFSDIR_TOKEN = "currentHDFSDir";

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
            List<String> lukeFacetFieldNames = searchService.getFacetFieldsFromLuke(coreName, true).getNames();
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

        // Save in the session for exporting
        HttpSession session = request.getSession();
        HashMap<String, String> searchParams = new HashMap<String, String>();
        searchParams.put(SESSION_SEARCH_QUERY, query);
        searchParams.put(SESSION_SEARCH_FQ, fq);
        searchParams.put(SESSION_SEARCH_CORE_NAME, coreName);
        session.setAttribute(SESSION_SEARCH_PARAMS, searchParams);

        FacetFieldEntryList facetFields = getFacetFields(coreName, session);
        StringWriter writer = new StringWriter();
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
                                          @RequestParam(value = PARAM_FIELD_SUGGEST_ENDPOINT, required = true) String field) throws IOException {

        JSONObject suggestions = searchService.suggest(coreName, userInput, field);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(suggestions.toString(), httpHeaders, OK);
    }
}
