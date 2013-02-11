package GatesBigData.api;

import GatesBigData.utils.Constants;
import net.sf.json.JSONObject;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import service.CoreService;
import service.HDFSService;
import service.SolrService;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/solr")
public class SolrAPIController extends APIController {

    public SolrService solrService;
    public CoreService coreService;
    public HDFSService hdfsService;

    @Autowired
    public SolrAPIController(SolrService solrService, CoreService coreService, HDFSService hdfsService) {
        this.solrService = solrService;
        this.coreService = coreService;
        this.hdfsService = hdfsService;
    }

    // Hackity hack hack
    public String getCoreTitle(String coreName) {
        if (coreName.equals("AR_data")) return "AR Data";
        if (coreName.equals("NA_data")) return "AP Data";
        if (coreName.equals("dnmsfp1")) return "dnmsfp1 crawl";
        if (coreName.equals("test2_data")) return "Unstructured Data Test";
        return coreName;
    }

    @RequestMapping(value="/corenames", method = RequestMethod.GET)
    public ResponseEntity<String> getCollectionNames() throws IOException {
        List<String> coreDisplayNames = new ArrayList<String>();

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("cores");
        for(String name : solrService.getCoreNames()) {
            if (name.equals(Constants.SOLR_THUMBNAILS_CORE_NAME)) continue;

            g.writeStartObject();
            g.writeStringField("name", name);
            g.writeStringField("title", getCoreTitle(name));
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
    public ResponseEntity<String> addInfoFiles() {

        JSONObject response = new JSONObject();
        for(String coreName : solrService.getCoreNames()) {
            boolean added = coreService.addInfoFilesToSolr(coreName, hdfsService.getInfoFilesContents(coreName));
            response.put(coreName, added);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info/all", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfoAll() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(solrService.getAllCoreData().toString(), httpHeaders, OK);
    }

}
