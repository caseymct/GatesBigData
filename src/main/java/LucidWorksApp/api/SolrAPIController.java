package LucidWorksApp.api;

import LucidWorksApp.utils.Constants;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.SolrService;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

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

    @RequestMapping(value="/corenames", method = RequestMethod.GET)
    public ResponseEntity<String> getCollectionNames() throws IOException {

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("names");

        for(String name : solrService.getCoreNames()) {
            g.writeString(name);
        }

        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info/all", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfoAll() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(solrService.getAllCoreData().toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/update/csv", method = RequestMethod.GET)
    public ResponseEntity<String> execQuery(@RequestParam(value = PARAM_FILE_NAME, required = true) String fileName,
                                            @RequestParam(value = PARAM_FILE_ONSERVER, required = true) Boolean fileOnServer,
                                            @RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName) throws IOException {

        String response = fileOnServer ? solrService.importCsvFileOnServerToSolr(collectionName, fileName) :
                                    solrService.importCsvFileOnLocalSystemToSolr(collectionName, fileName);

        //curl 'http://localhost:8080/wikipedia/update/csv?commit=false&overwrite=false&separator=%09&encapsulator=%1f&header=false&fieldnames=id,title,updated,xml,text&skip=updated,xml&stream.file=/wex/rawd/freebase-wex-2009-01-12-articles.tsv&stream.contentType=text/plain;charset=utf-8'

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response, httpHeaders, OK);
    }


}
