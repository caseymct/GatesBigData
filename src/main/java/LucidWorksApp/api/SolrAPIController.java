package LucidWorksApp.api;

import LucidWorksApp.search.SolrUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/solr")
public class SolrAPIController extends APIController {
    public static final String PARAM_FILENAME = "file";
    public static final String PARAM_LOCAL = "local";

    @RequestMapping(value="/update/csv", method = RequestMethod.GET)
    public ResponseEntity<String> execQuery(@RequestParam(value = PARAM_FILENAME, required = true) String fileName,
                                            @RequestParam(value = PARAM_LOCAL, required = true) Boolean isLocal,
                                            @RequestParam(value = PARAM_COLLECTION_NAME, required = true) String collectionName) throws IOException {

        String response = isLocal ? SolrUtils.importLocalCsvToSolr(collectionName, fileName) :
                SolrUtils.importRemoteCsvToSolr(collectionName, fileName);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response, httpHeaders, OK);
    }
}
