package LucidWorksApp.api;

import org.codehaus.jackson.JsonFactory;
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

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;


@Controller
@RequestMapping("/search")
public class SearchAPIController extends APIController {

    public static final String PARAM_QUERY = "query";
    public static final String PARAM_START = "start";
    public static final String PARAM_SORT_TYPE = "sort";
    public static final String PARAM_SORT_ORDER = "order";
    public static final String PARAM_FQ = "fq";

    private SearchService searchService;

    @Autowired
    public SearchAPIController(SearchService searchService) {
        this.searchService = searchService;
    }

    //http://localhost:8080/LucidWorksApp/search/solrquery?query=*:*&core=collection1t&sort=score&order=desc&start=0
    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String query,
                                            @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                            @RequestParam(value = PARAM_SORT_TYPE, required = true) String sortType,
                                            @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                            @RequestParam(value = PARAM_START, required = false) Integer start,
                                            @RequestParam(value = PARAM_FQ, required = false) String fq) throws IOException {

        if (start == null) {
            start = 0;
        }
        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeObjectFieldStart("response");

        searchService.solrSearch(query, coreName, sortType, sortOrder, start, fq, g);

        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
