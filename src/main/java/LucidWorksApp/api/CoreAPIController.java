package LucidWorksApp.api;

import LucidWorksApp.utils.CoreUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
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
import service.CoreService;
import service.HDFSService;
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
    private CoreService coreService;
    private HDFSService hdfsService;

    @Autowired
    public CoreAPIController(SolrService solrService, CoreService coreService, HDFSService hdfsService) {
        this.solrService = solrService;
        this.coreService = coreService;
        this.hdfsService = hdfsService;
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

    @RequestMapping(value="/info", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfo(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(coreService.getCoreData(coreName).toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info/all", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfoAll() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(solrService.getAllCoreData().toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/empty", method = RequestMethod.GET)
    public ResponseEntity<String> deleteIndex(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        boolean deleted = coreService.deleteIndex(coreName);

        JSONObject response = new JSONObject();
        response.put("Core", coreName);
        response.put("Deleted", deleted);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/addfile/json", method = RequestMethod.GET)
    public ResponseEntity<String> addDocument(@RequestParam(value = PARAM_FILENAME, required = true) String fileName,
                                              @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                              @RequestParam(value = PARAM_HDFS, required = true) String hdfsKey) {
        boolean added = false;

        JSONObject response = new JSONObject();
        response.put("File", fileName);
        response.put("Core", coreName);

        String jsonDocument = Utils.readFileIntoString(fileName);

        if (!jsonDocument.equals("")) {
            try {
                JSONObject jsonObject = JSONObject.fromObject(jsonDocument);
                added = coreService.addDocumentToSolr(jsonObject, hdfsKey, coreName);
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

    @RequestMapping(value="/create", method = RequestMethod.POST)
    public ResponseEntity<String> createCore(@RequestBody String body) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> newDataSource = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        String coreName = (String) newDataSource.get("coreName");
        HashMap<String, Object> properties = new HashMap<String, Object>();
        properties.put("name", coreName);

        String result = CoreUtils.createCore(properties);
        //http://localhost:8983/solr/admin/cores?action=CREATE&name=coreX&instanceDir=path_to_instance_directory&config=config_file_name.xml&schema=schem_file_name.xml&dataDir=data

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }



    @RequestMapping(value="/repopulate", method = RequestMethod.GET)
    public ResponseEntity<String> repopulateCoreFromHDFS(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                         @RequestParam(value = PARAM_HDFS, required = true) String hdfsDir) {
        JSONObject response = new JSONObject();

        boolean deleted = coreService.deleteIndex(coreName);
        if (!deleted) {
            response.put("Error", "Could not delete index for core " + coreName);

        } else {
            /*
            for(String hdfsFilePath : hdfsService.listFiles(hdfsDir)) {
                boolean added = false;

                if (hdfsFilePath.endsWith(".json")) {
                    JSONObject contents = hdfsService.getJSONFileContents(hdfsFilePath);
                    added = solrService.addJsonDocumentToSolr(contents, coreName, hdfsFilePath);
                } else {
                    String contents = hdfsService.getFileContents(hdfsFilePath);
                    added = solrService.addDocumentToSolr(contents, coreName, hdfsFilePath);
                }

                response.put("Added " + hdfsFilePath, added);
            }
            */
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }
}
