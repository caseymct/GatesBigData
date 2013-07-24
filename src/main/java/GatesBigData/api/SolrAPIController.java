package GatesBigData.api;

import model.schema.CollectionSchemaInfo;
import model.schema.CollectionsConfig;
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
import service.CollectionService;
import service.HDFSService;
import service.SearchService;
import service.SolrService;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static GatesBigData.utils.Utils.writeValueByType;
import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;
import static GatesBigData.constants.solr.Operations.*;
import static GatesBigData.constants.solr.Response.*;
import static GatesBigData.constants.solr.FieldNames.*;

@Controller
@RequestMapping("/solr")
public class SolrAPIController extends APIController {

    public SolrService solrService;
    public CollectionService collectionService;
    public SearchService searchService;
    public HDFSService hdfsService;

    @Autowired
    public CollectionsConfig schemaInfo;

    @Autowired
    public SolrAPIController(SolrService solrService, CollectionService collectionService, SearchService searchService, HDFSService hdfsService) {
        this.solrService   = solrService;
        this.collectionService = collectionService;
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
        g.writeArrayFieldStart(COLLECTIONS_KEY);
        for(String name : solrService.getCollectionNames()) {
            if (skip(name)) continue;

            CollectionSchemaInfo info = schemaInfo.getSchema(name);
            g.writeStartObject();
            writeValueByType(COLLECTION_KEY, name, g);
            writeValueByType(TITLE_KEY,      info.getCoreTitle(), g);
            writeValueByType(STRUCTURED_KEY, info.isStructuredData(), g);
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/optimize", method = RequestMethod.GET)
    public ResponseEntity<String> optimize(@RequestParam(value = PARAM_COLLECTION, required = false) String collection) {

        StringWriter writer = new StringWriter();
        for(String c : getCollectionNames(collection)) {
            collectionService.doSolrOperation(c, OPERATION_OPTIMIZE, writer);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info/all", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfoAll() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        JSONObject collectionInfo = solrService.getCollectionInfo();
        Iterator<?> keys = collectionInfo.keys();

        while (keys.hasNext()){
            String name = (String) keys.next();
            CollectionSchemaInfo info = schemaInfo.getSchema(name);
            JSONObject o = collectionInfo.getJSONObject(name);
            o.put(TITLE,           info.getCoreTitle());
            o.put(STRUCTURED_DATA, info.isStructuredData());
            collectionInfo.put(name, o);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(collectionInfo.toString(), httpHeaders, OK);
    }
}