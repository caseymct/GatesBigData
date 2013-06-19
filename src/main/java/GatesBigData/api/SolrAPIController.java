package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.Operations;
import GatesBigData.constants.solr.Response;
import GatesBigData.constants.solr.Solr;
import GatesBigData.utils.Utils;
import model.SolrSchemaInfo;
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
import service.CoreService;
import service.HDFSService;
import service.SearchService;
import service.SolrService;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/solr")
public class SolrAPIController extends APIController {

    public SolrService solrService;
    public CoreService coreService;
    public SearchService searchService;
    public HDFSService hdfsService;

    @Autowired
    public SolrSchemaInfo schemaInfo;

    @Autowired
    public SolrAPIController(SolrService solrService, CoreService coreService, SearchService searchService, HDFSService hdfsService) {
        this.solrService   = solrService;
        this.coreService   = coreService;
        this.hdfsService   = hdfsService;
        this.searchService = searchService;
    }

    private List<String> getCollectionNames(String collection) {
        return collection == null ? solrService.getCollectionNames() : Arrays.asList(collection);
    }

    @RequestMapping(value="/collection/names", method = RequestMethod.GET)
    public ResponseEntity<String> getCollectionNames() throws IOException {

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart(Response.COLLECTIONS_KEY);
        for(String name : solrService.getCollectionNames()) {
            if (Solr.skip(name)) continue;

            g.writeStartObject();
            Utils.writeValueByType(Response.COLLECTION_KEY, name, g);
            Utils.writeValueByType(Response.TITLE_KEY,      schemaInfo.getCoreTitle(name), g);
            Utils.writeValueByType(Response.STRUCTURED_KEY, schemaInfo.isStructuredData(name), g);
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/add/infofiles", method = RequestMethod.GET)
    public ResponseEntity<String> addInfoFiles(@RequestParam(value = PARAM_COLLECTION, required = false) String collection) {

        StringWriter writer = new StringWriter();
        for(String c : getCollectionNames(collection)) {
            if (Solr.skip(c)) continue;

            coreService.doSolrOperation(c, Operations.OPERATION_ADD_INFOFILES, hdfsService.getInfoFileContents(c), writer);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }


    @RequestMapping(value="/optimize", method = RequestMethod.GET)
    public ResponseEntity<String> optimize(@RequestParam(value = PARAM_COLLECTION, required = false) String collection) {

        StringWriter writer = new StringWriter();
        for(String c : getCollectionNames(collection)) {
            coreService.doSolrOperation(c, Operations.OPERATION_OPTIMIZE, null, writer);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info/all", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfoAll() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));

        JSONObject collectionInfo = solrService.getCollectionInfo();
        Iterator<?> keys = collectionInfo.keys();

        while (keys.hasNext()){
            String name = (String) keys.next();

            JSONObject collectionJsonObject = collectionInfo.getJSONObject(name);
            collectionJsonObject.put(FieldNames.TITLE,           schemaInfo.getCoreTitle(name));
            collectionJsonObject.put(FieldNames.STRUCTURED_DATA, schemaInfo.isStructuredData(name));
            collectionInfo.put(name, collectionJsonObject);
        }
        return new ResponseEntity<String>(collectionInfo.toString(), httpHeaders, OK);
    }
}