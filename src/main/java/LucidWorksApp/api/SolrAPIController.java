package LucidWorksApp.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.SolrService;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/solr")
public class SolrAPIController extends APIController {

    public SolrService solrService;

    @Autowired
    public SolrAPIController(SolrService solrService) {
        this.solrService = solrService;
    }

    @RequestMapping(value="/update/csv", method = RequestMethod.GET)
    public ResponseEntity<String> execQuery(@RequestParam(value = PARAM_FILE_NAME, required = true) String fileName,
                                            @RequestParam(value = PARAM_FILE_ONSERVER, required = true) Boolean fileOnServer,
                                            @RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName) throws IOException {

        String response = fileOnServer ? solrService.importCsvFileOnServerToSolr(collectionName, fileName) :
                                    solrService.importCsvFileOnLocalSystemToSolr(collectionName, fileName);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response, httpHeaders, OK);
    }


}
