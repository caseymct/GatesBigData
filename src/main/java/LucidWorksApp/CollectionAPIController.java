package LucidWorksApp;

import LucidWorksApp.utils.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/collection")
public class CollectionAPIController extends APIController {

    @RequestMapping(value="/index", method = RequestMethod.GET)
    public String test(ModelMap modelMap) throws IOException {
        modelMap.addAttribute("test", "test");
        return "index";
    }


    //http://localhost:8080/LucidWorksApp/collection/collectionNames
    @RequestMapping(value="/collectionNames", method = RequestMethod.GET)
    public ResponseEntity<String> getCollectionNames() throws IOException {

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("names");

        for(String name : CollectionUtils.getCollectionNames()) {
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

        for(String name : CollectionUtils.getCollectionNames()) {
            g.writeStartObject();
            g.writeStringField("name", name);
            g.writeNumberField("docs", (Integer) CollectionUtils.getCollectionProperty(name, "index_num_docs"));
            g.writeStringField("size", (String) CollectionUtils.getCollectionProperty(name, "index_size"));
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
}
