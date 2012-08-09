package LucidWorksApp.api;

import LucidWorksApp.api.APIController;
import LucidWorksApp.utils.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/datasource")
public class DatasourceAPIController extends APIController {


    public static final String PARAM_DATASOURCE_ID = "datasourceId";

    @RequestMapping(value="/topleveldetails", method = RequestMethod.GET)
    public ResponseEntity<String> getAllDatasources(@RequestParam(value = PARAM_COLLECTION_NAME, required = true) String collectionName) throws IOException {

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("datasources");

        HashMap<String, Integer> dataSourceNamesAndIds = DatasourceUtils.getDataSourceNamesAndIds(collectionName);
        if (dataSourceNamesAndIds.isEmpty()) {
            g.writeStartObject();
            g.writeStringField("name", "None");
            g.writeStringField("crawl_state", "None");
            g.writeNumberField("docs", 0);
            g.writeStringField("type", "None");
            g.writeStringField("url", "None");
            g.writeNumberField("id", -1);
            g.writeEndObject();
        } else {
            for(Map.Entry entry : dataSourceNamesAndIds.entrySet()) {
                Integer datasourceId = (Integer) entry.getValue();

                g.writeStartObject();
                Utils.writeValueByType("name", entry.getKey(), g);

                for(Map.Entry dsEntry : DatasourceUtils.getDataSourceProperties(collectionName,
                        datasourceId, Arrays.asList("type", "url", "id")).entrySet()) {
                    Utils.writeValueByType((String) dsEntry.getKey(), dsEntry.getValue(), g);
                }

                Utils.writeValueByType("crawl_state",
                        DatasourceUtils.getDataSourceStatusProperty(collectionName, datasourceId, "crawl_state"), g);
                Utils.writeValueByType("docs",
                        DatasourceUtils.getDataSourceHistoryProperty(collectionName, datasourceId, "num_total"), g);

                //  g.writeNumberField("dataSources", DatasourceUtils.getNumberOfDataSources(name));
                //  g.writeStringField("crawling", CrawlingUtils.getCrawlerStatus(name));
                g.writeEndObject();
            }
        }

        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/datasourcedetails", method = RequestMethod.GET)
    public ResponseEntity<String> getDatasourceDetails(@RequestParam(value = PARAM_COLLECTION_NAME, required = true) String collectionName,
                                                       @RequestParam(value = PARAM_DATASOURCE_ID, required = true) Integer datasourceId) throws IOException {

        String result = DatasourceUtils.getAllDataSourceProperties(collectionName, datasourceId);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }

    @RequestMapping(value="/create", method = RequestMethod.POST)
    public ResponseEntity<String> createDatasource(@RequestBody String body) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> newDataSource = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        String collectionName = (String) newDataSource.get("collectionName");
        HashMap<String, Object> properties = (HashMap<String, Object>) newDataSource.get("properties");

        String result = DatasourceUtils.createDatasource(collectionName, properties);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }

    @RequestMapping(value = "/delete", method = RequestMethod.DELETE)
    public ResponseEntity<String> deleteDatassource(@RequestParam(value = PARAM_COLLECTION_NAME, required = true) String collectionName,
                                                    @RequestParam(value = PARAM_DATASOURCE_ID, required = true) Integer datasourceId) {
        String result = DatasourceUtils.deleteDatasource(collectionName, datasourceId.toString());

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(result, httpHeaders, OK);
    }
}
