package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.Operations;
import GatesBigData.constants.solr.Solr;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.SolrUtils;
import model.SolrCollectionSchemaInfo;
import model.SolrSchemaInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
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
    private SearchService searchService;
    private HDFSService hdfsService;
    private SolrSchemaInfo schemaInfo;

    @Autowired
    public CoreAPIController(CoreService coreService, SearchService searchService,
                             HDFSService hdfsService, SolrSchemaInfo schemaInfo) {
        this.coreService = coreService;
        this.searchService = searchService;
        this.hdfsService = hdfsService;
        this.schemaInfo = schemaInfo;
    }

    @RequestMapping(value="/fieldnames", method = RequestMethod.GET)
    public ResponseEntity<String> getFieldNames(@RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName)
            throws IOException {

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("names");

        for(String name : schemaInfo.getViewFieldNames(collectionName)) {
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
    public ResponseEntity<String> coreInfo(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(coreService.getCollectionInfo(coreName).toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/update", method = RequestMethod.GET)
    public ResponseEntity<String> updateDoc(@RequestParam(value = PARAM_COLLECTION, required = true) String collection) {
        String id = "de7b3c36-3990-3592-ba5a-b559cafeeb8a";
        SolrDocument oldDoc = searchService.getRecordById(collection, id);

        List<String> values = new ArrayList<String>();
        for(String key : Arrays.asList("SEGMENT1","SEGMENT2","SEGMENT3","SEGMENT4","SEGMENT5","SEGMENT6","SEGMENT7")) {
            values.add(SolrUtils.getFieldStringValue(oldDoc, key, ""));
        }
        String newAcct = StringUtils.join(values, ".");

        Map<String,String> oper = new HashMap<String,String>();
        oper.put("set", newAcct);
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("ACCOUNT", oper);
        ConcurrentUpdateSolrServer server = new ConcurrentUpdateSolrServer(Solr.SOLR_SERVER + "/" + collection, 200, 4);
        try {
            server.add(doc);
            server.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }


        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>("", httpHeaders, OK);
    }

    @RequestMapping(value="/empty", method = RequestMethod.GET)
    public ResponseEntity<String> deleteIndex(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        StringWriter writer = new StringWriter();
        coreService.doSolrOperation(coreName, Operations.OPERATION_DELETE, null, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/add/infofiles", method = RequestMethod.GET)
    public ResponseEntity<String> addFacetFields(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {

        StringWriter writer = new StringWriter();
        coreService.doSolrOperation(coreName, Operations.OPERATION_ADD_INFOFILES,
                                    hdfsService.getInfoFileContents(coreName), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
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

        String coreName = (String) newDataSource.get("collectionName");
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

    @RequestMapping(value="/field/daterange", method = RequestMethod.GET)
    public ResponseEntity<String> dateRange(@RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                            @RequestParam(value = PARAM_FIELD_NAME, required = true) String field) {

        SolrCollectionSchemaInfo info = schemaInfo.getSchema(collection);
        List<Date> dateRange = searchService.getSolrFieldDateRange(collection, field, info);
        String dateString = DateUtils.getFormattedDateString(dateRange.get(0), DateUtils.SHORT_DATE_FORMAT) + " to " +
                            DateUtils.getFormattedDateString(dateRange.get(1), DateUtils.SHORT_DATE_FORMAT);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(dateString, httpHeaders, OK);
    }
}
