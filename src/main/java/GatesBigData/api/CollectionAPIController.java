package GatesBigData.api;

import model.schema.CollectionSchemaInfo;
import model.schema.CollectionsConfig;
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
import service.SearchService;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import static GatesBigData.utils.DateUtils.*;
import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;
import static GatesBigData.constants.Constants.*;
import static GatesBigData.utils.Utils.*;
import static GatesBigData.constants.solr.Response.*;
import static GatesBigData.constants.solr.Operations.*;

@Controller
@RequestMapping("/collection")
public class CollectionAPIController extends APIController {

    private CollectionService collectionService;
    private SearchService searchService;
    private CollectionsConfig collectionsConfig;

    @Autowired
    public CollectionAPIController(CollectionService collectionService, SearchService searchService,
                                   CollectionsConfig collectionsConfig) {
        this.collectionService = collectionService;
        this.searchService = searchService;
        this.collectionsConfig = collectionsConfig;
    }

    @RequestMapping(value="/fieldnames", method = RequestMethod.GET)
    public ResponseEntity<String> getFieldNames(@RequestParam(value = PARAM_COLLECTION, required = true) String collection)
            throws IOException {

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        writeJSONArray(FIELDNAMES_KEY, collectionsConfig.getSchemaViewFields(collection), g);
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info", method = RequestMethod.GET)
    public ResponseEntity<String> collectionInfo(@RequestParam(value = PARAM_COLLECTION, required = true) String collection) {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));

        return new ResponseEntity<String>(collectionService.getCollectionInfo(collection).toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/empty", method = RequestMethod.GET)
    public ResponseEntity<String> deleteIndex(@RequestParam(value = PARAM_COLLECTION, required = true) String coreName) {

        StringWriter writer = new StringWriter();
        collectionService.doSolrOperation(coreName, OPERATION_DELETE, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/field/daterange", method = RequestMethod.GET)
    public ResponseEntity<String> dateRange(@RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                            @RequestParam(value = PARAM_FIELD_NAME, required = true) String field) {

        CollectionSchemaInfo schema = collectionsConfig.getSchema(collection);
        List<Date> dateRange = searchService.getSolrFieldDateRange(collection, field, schema);
        String dateString = getFormattedDateString(dateRange.get(0), SHORT_DATE_FORMAT) + " to " +
                            getFormattedDateString(dateRange.get(1), SHORT_DATE_FORMAT);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(dateString, httpHeaders, OK);
    }
}
