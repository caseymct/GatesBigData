package LucidWorksApp.api;

import LucidWorksApp.api.APIController;
import LucidWorksApp.search.SolrUtils;
import LucidWorksApp.utils.CollectionUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.io.StringWriter;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;


@Controller
@RequestMapping("/search")
public class SearchAPIController extends APIController {

    public static final String PARAM_QUERY = "query";
    public static final String PARAM_COLLECTION_NAME = "collection";
    public static final String PARAM_START = "start";
    public static final String PARAM_SORT_TYPE = "sort";
    public static final String PARAM_SORT_ORDER = "order";
    public static final String PARAM_FQ = "fq";

    @RequestMapping(value="/query", method = RequestMethod.GET)
    public ResponseEntity<String> execLucidQuery(@RequestParam(value = PARAM_QUERY, required = true) String queryString,
                                            @RequestParam(value = PARAM_COLLECTION_NAME, required = true) String collectionName,
                                            @RequestParam(value = PARAM_SORT_TYPE, required = true) String sortType,
                                            @RequestParam(value = PARAM_START, required = false) String start) throws IOException {
        String queryParams = "?q=" + queryString + "&sort=" + sortType + "+desc";
        if (start != null) {
            queryParams += "&start=" + start;
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(SolrUtils.lucidEndpointSearch(collectionName, queryParams), httpHeaders, OK);
    }

    @RequestMapping(value="/solrquery", method = RequestMethod.GET)
    public ResponseEntity<String> execSolrQuery(@RequestParam(value = PARAM_QUERY, required = true) String queryString,
                                            @RequestParam(value = PARAM_COLLECTION_NAME, required = true) String collectionName,
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

        SolrUtils.solrSearch(collectionName, queryString, sortType, sortOrder, start, fq, g);

        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
