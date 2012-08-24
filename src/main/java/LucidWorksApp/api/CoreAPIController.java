package LucidWorksApp.api;

import LucidWorksApp.utils.CoreUtils;
import LucidWorksApp.utils.CrawlingUtils;
import LucidWorksApp.utils.DatasourceUtils;
import net.sf.json.JSONObject;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
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
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/core")
public class CoreAPIController extends APIController {

    private SolrService solrService;

    @Autowired
    public CoreAPIController(SolrService solrService) {
        this.solrService = solrService;
    }


    //http://localhost:8080/LucidWorksApp/core/corenames
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
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/collectionOverview", method = RequestMethod.GET)
    public ResponseEntity<String> getCollectionOverview() throws IOException {
        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("collections");

        for(String name : CoreUtils.getCoreNames()) {
            g.writeStartObject();
            g.writeStringField("name", name);
            g.writeNumberField("docs", (Integer) CoreUtils.getCollectionProperty(name, "index_num_docs"));
            g.writeStringField("size", (String) CoreUtils.getCollectionProperty(name, "index_size"));
            g.writeNumberField("dataSources", DatasourceUtils.getNumberOfDataSources(name));
            g.writeStringField("crawling", CrawlingUtils.getCrawlerStatus(name));
            g.writeEndObject();
        }

        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/create", method = RequestMethod.POST)
    public ResponseEntity<String> createCollection(@RequestBody String body) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> newDataSource = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        String collectionName = (String) newDataSource.get("collectionName");
        HashMap<String, Object> properties = new HashMap<String, Object>();
        properties.put("name", collectionName);

        String result = CoreUtils.createCollection(properties);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }

    @RequestMapping(value="/info", method = RequestMethod.GET)
    public ResponseEntity<String> collectionInfo(@RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(CoreUtils.getCollectionInfo(collectionName), httpHeaders, OK);
    }

    @RequestMapping(value="/delete", method = RequestMethod.DELETE)
    public ResponseEntity<String> deleteCollection(@RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName) {
        System.out.println("here");
        String result = CoreUtils.deleteCollection(collectionName);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }

    @RequestMapping(value="/empty", method = RequestMethod.GET)
    public ResponseEntity<String> deleteIndex(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        boolean deleted = solrService.deleteIndex(coreName);

        JSONObject response = new JSONObject();
        response.put("Core", coreName);
        response.put("Deleted", deleted);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }
}
