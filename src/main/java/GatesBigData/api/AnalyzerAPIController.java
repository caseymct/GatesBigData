package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.JsonParsingUtils;
import model.WordTree;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
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

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/analyze")
public class AnalyzerAPIController extends APIController {

    private static final String ID_TO_TITLE_MAP_KEY = "idToTitleMap";

    private static final Logger logger = Logger.getLogger(AnalyzerAPIController.class);

    // highlighting: {
    //      key {
    //          field : [ values ]
    //          field : [ values ]
    //      }, ...
    public Map<String, HashMap<String, String>> reconfigureHighlighting(JSONObject hlJson) {
        Map<String, HashMap<String,String>> highlighting = new HashMap<String, HashMap<String, String>>();

        for(Object keyObj : hlJson.names()) {
            String id = keyObj.toString();
            JSONObject docObj = hlJson.getJSONObject(id);

            for(Object docKeyObj : docObj.names()) {
                String field = docKeyObj.toString();
                List<String> snippetStrings = JsonParsingUtils.convertJSONArrayToStringList(docObj.getJSONArray(field));

                HashMap<String, String> hlToIdMap = new HashMap<String, String>();
                if (highlighting.containsKey(field)) {
                    hlToIdMap = highlighting.get(field);
                }
                for(String snippetString : snippetStrings) {
                    hlToIdMap.put(snippetString, id);
                }
                highlighting.put(field, hlToIdMap);
            }
        }
        return highlighting;
    }

    @RequestMapping(value="/snippets", method = RequestMethod.POST)
    public ResponseEntity<String> analyze(@RequestBody String body) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        StringWriter writer = new StringWriter();
        String hlResponse = (String) map.get(Constants.SOLR_HIGHLIGHT_PARAM);
        JSONObject hlJson = JSONObject.fromObject(hlResponse);

        String hlPre            = (String) hlJson.remove(Constants.SOLR_HIGHLIGHT_PRE_PARAM);
        String hlPost           = (String) hlJson.remove(Constants.SOLR_HIGHLIGHT_POST_PARAM);
        JSONObject idToTitleMap = (JSONObject) hlJson.remove(ID_TO_TITLE_MAP_KEY);

        Map<String, HashMap<String, String>> highlighting = reconfigureHighlighting(hlJson);

        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);
        g.writeStartObject();

        for(Map.Entry<String, HashMap<String, String>> entry : highlighting.entrySet()) {
            String field = entry.getKey();
            WordTree tree = new WordTree(field, hlPre, hlPost, idToTitleMap);

            tree.addSnippets(entry.getValue());
            tree.combine();
            tree.printTree(g);
        }

        g.writeEndObject();
        g.close();
        g.flush();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/wordtreetest", method = RequestMethod.GET)
    public ResponseEntity<String> test() throws IOException {

        WordTree tree = new WordTree("test", "<b>", "</b>", null);

        /*tree.addSnippet("here is a <b>test</b>ing one two three");
        tree.addSnippet("what is a <b>test</b>: one four five three");
        tree.addSnippet("this isn't a <b>test</b> one four five seven");
        tree.addSnippet("why <b>test</b> three four five three");
        tree.addSnippet("<b>test</b> three four three");       */
        StringWriter writer = new StringWriter();

        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);
        g.writeStartObject();

        tree.combine();
        tree.printTree(g);

        g.writeEndObject();
        g.close();
        g.flush();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
