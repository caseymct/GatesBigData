package GatesBigData.api;

import GatesBigData.utils.*;
import model.SolrCollectionSchemaInfo;
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
import service.SearchService;
import service.SolrReindexService;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/core")
public class CoreAPIController extends APIController {

    private CoreService coreService;
    private SolrReindexService solrReindexService;
    private HDFSService hdfsService;
    private SearchService searchService;

    @Autowired
    public CoreAPIController(CoreService coreService, SolrReindexService solrReindexService,
                             HDFSService hdfsService, SearchService searchService) {
        this.coreService = coreService;
        this.solrReindexService = solrReindexService;
        this.hdfsService = hdfsService;
        this.searchService = searchService;
    }

    @RequestMapping(value="/fieldnames", method = RequestMethod.GET)
    public ResponseEntity<String> getFieldNames(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                HttpServletRequest request) throws IOException {

        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreService.getSolrServer(coreName),
                coreName, request.getSession());
        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("names");

        for(String name : schemaInfo.getViewFieldNames()) {
            g.writeString(name);
        }

        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfo(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                           HttpServletRequest request)
            throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreService.getSolrServer(coreName),
                coreName, request.getSession());

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(schemaInfo.getIndexInfoAsJSON().toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/empty", method = RequestMethod.GET)
    public ResponseEntity<String> deleteIndex(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        boolean deleted = coreService.deleteIndex(coreName);

        JSONObject response = new JSONObject();
        response.put("Core", coreName);
        response.put("Deleted", deleted);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/add/infofiles", method = RequestMethod.GET)
    public ResponseEntity<String> addFacetFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        boolean added = coreService.addInfoFilesToSolr(coreName, hdfsService.getInfoFilesContents(coreName));
        JSONObject response = new JSONObject();
        response.put("Added", added);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/addfile/json", method = RequestMethod.GET)
    public ResponseEntity<String> addDocument(@RequestParam(value = PARAM_FILE_NAME, required = true) String fileName,
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
                added = coreService.createAndAddDocumentToSolr(jsonObject, hdfsKey, coreName);
            } catch (JSONException e) {
                System.err.println(e.getMessage());
            }
        }
        response.put("Added", added);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/add/json", method = RequestMethod.POST)
    public ResponseEntity<String> addDocument(@RequestBody String body) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        //response = solrService.addJsonDocumentToSolr(jsonDocument, "")
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>("", httpHeaders, OK);
    }

    @RequestMapping(value="/create", method = RequestMethod.POST)
    public ResponseEntity<String> createCore(@RequestBody String body) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> newDataSource = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        //curl 'http://localhost:8984/solr/admin/collections?action=CREATE&name=$name&numShards=$nshards&repl
        //icationFactor=0'

        String coreName = (String) newDataSource.get("coreName");
        int nShards = (Integer) newDataSource.get("nShards");
        int replicationFactor = (Integer) newDataSource.get("replicationFactor");

        HashMap<String, Object> properties = new HashMap<String, Object>();
        properties.put("name", coreName);
        properties.put("action", "CREATE");
        properties.put("numShards", nShards);
        properties.put("replicationFactor", replicationFactor);

        String result = "";
        //http://localhost:8983/solr/admin/cores?action=CREATE&name=coreX&instanceDir=path_to_instance_directory&config=config_file_name.xml&schema=schem_file_name.xml&dataDir=data

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }



    @RequestMapping(value="/reindex", method = RequestMethod.GET)
    public ResponseEntity<String> repopulateCoreFromHDFS(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                         @RequestParam(value = PARAM_N_THREADS, required = false) Integer nThreads,
                                                         @RequestParam(value = PARAM_N_FILES, required = false) Integer nFiles) {
        JSONObject response = new JSONObject();

        //boolean deleted = coreService.deleteIndex(coreName);
        //if (!deleted) {
        //    response.put("Error", "Could not delete index for core " + coreName);
        //} else {
            solrReindexService.reindexSolrCoreFromHDFS(coreName, (nThreads == null) ? -1 : nThreads, (nFiles == null) ? -1 : nFiles);
            response.put("Success", "Successfully reindexed " + coreName);
        //}

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/field/daterange", method = RequestMethod.GET)
    public ResponseEntity<String> dateRange(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                            @RequestParam(value = PARAM_FIELD_NAME, required = true) String field) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        List<String> dateRange = coreService.getSolrFieldDateRange(coreName, field, DateUtils.SHORT_DATE);
        String dateString = dateRange.get(0) + " to " + dateRange.get(1);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(dateString, httpHeaders, OK);
    }
}
