package LucidWorksApp.api;

import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.SolrService;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

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
    public ResponseEntity<String> execQuery(@RequestParam(value = PARAM_FILENAME, required = true) String fileName,
                                            @RequestParam(value = PARAM_FILE_ONSERVER, required = true) Boolean fileOnServer,
                                            @RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName) throws IOException {

        String response = fileOnServer ? solrService.importCsvFileOnServerToSolr(collectionName, fileName) :
                                    solrService.importCsvFileOnLocalSystemToSolr(collectionName, fileName);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response, httpHeaders, OK);
    }

    @RequestMapping(value="/addfile/json", method = RequestMethod.GET)
    public ResponseEntity<String> addDocument(@RequestParam(value = PARAM_FILENAME, required = true) String fileName,
                                              @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                              @RequestParam(value = PARAM_HDFS_KEY, required = true) String hdfsKey) {
        boolean added = false;

        JSONObject response = new JSONObject();
        response.put("File", fileName);
        response.put("Core", coreName);

        String jsonDocument = Utils.readFileIntoString(fileName);

        if (!jsonDocument.equals("")) {
            try {
                JSONObject jsonObject = JSONObject.fromObject(jsonDocument);
                added = solrService.addJsonDocumentToSolr(jsonObject, coreName, hdfsKey);
            } catch (JSONException e) {
                System.err.println(e.getMessage());
            }
        }
        response.put("Added", added);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/add/json", method = RequestMethod.POST)
    public ResponseEntity<String> addDocument(@RequestBody String body) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));


        //response = solrService.addJsonDocumentToSolr(jsonDocument, "")
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>("", httpHeaders, OK);
    }
}
