package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.Defaults;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.QueryParams;
import GatesBigData.utils.JSONUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.schema.CollectionConfig;
import model.schema.CollectionSchemaInfo;
import model.schema.CollectionsConfig;
import model.analysis.WordTree;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
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
import service.SearchService;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import static GatesBigData.utils.Utils.*;
import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;
import static GatesBigData.constants.Constants.*;
import static GatesBigData.constants.solr.QueryParams.*;
import static GatesBigData.utils.SolrUtils.*;

@Controller
@RequestMapping("/analyze")
public class AnalyzerAPIController extends APIController {

    private static final Logger logger = Logger.getLogger(AnalyzerAPIController.class);
    private SearchService searchService;
    private CollectionsConfig collectionsConfig;

    @Autowired
    public AnalyzerAPIController(SearchService searchService, CollectionsConfig collectionsConfig) {
        this.searchService = searchService;
        this.collectionsConfig = collectionsConfig;
    }

    public Map<String, HashMap<String, String>> reconfigureHighlighting(JSONObject hlJson) {
        Map<String, HashMap<String,String>> highlighting = new HashMap<String, HashMap<String, String>>();

        for(Object keyObj : hlJson.names()) {
            String id = keyObj.toString();
            JSONObject docObj = hlJson.getJSONObject(id);

            for(Object docKeyObj : docObj.names()) {
                String field = docKeyObj.toString();
                List<String> snippetStrings = JSONUtils.convertJSONArrayToStringList(docObj.getJSONArray(field));

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

    public List<WordTree> getNonEmptyWordTrees(HashMap<String, WordTree> wordTreeHashMap) {
        List<WordTree> nonEmptyWordTrees = new ArrayList<WordTree>();
        for(Map.Entry<String, WordTree> entry : wordTreeHashMap.entrySet()) {
            WordTree tree = entry.getValue();
            if (!tree.isEmpty()) {
                nonEmptyWordTrees.add(tree);
            }
        }
        return nonEmptyWordTrees;
    }

    public List<WordTree> getWordTreesFromGroupResults(GroupResponse rsp) {

        HashMap<String, WordTree> wordTrees = new HashMap<String, WordTree>();
        if (rsp != null) {
            for(GroupCommand command : rsp.getValues()) {
                for(Group group : command.getValues()) {
                    String snippet = group.getGroupValue();
                    if (nullOrEmpty(snippet)) continue;

                    int idx = snippet.indexOf(" ");
                    String firstWord = (idx != -1 ? snippet.substring(0, idx) : snippet).toLowerCase();
                    SolrDocumentList docs = group.getResult();

                    WordTree wordTree = wordTrees.containsKey(firstWord) ? wordTrees.get(firstWord) : new WordTree(firstWord);
                    wordTree.addGroupQuerySnippet(snippet, docs.getNumFound());
                    wordTrees.put(firstWord, wordTree);
                }
            }
        }

        return getNonEmptyWordTrees(wordTrees);
    }

    public List<WordTree> getWordTreeFromSearchResults(QueryResponse rsp) {
        String hlPre  = SolrUtils.getResponseHeaderParam(rsp, HIGHLIGHT_PRE);
        String hlPost = SolrUtils.getResponseHeaderParam(rsp, QueryParams.HIGHLIGHT_POST);
        Map<String, Map<String, List<String>>> highlighting = rsp.getHighlighting();

        HashMap<String, WordTree> wordTrees = new HashMap<String, WordTree>();

        for(Map.Entry<String, Map<String, List<String>>> entry : highlighting.entrySet()) {
            for(Map.Entry<String, List<String>> fieldNameToInstances : entry.getValue().entrySet()) {
                String fieldName = fieldNameToInstances.getKey();
                List<String> snippets = fieldNameToInstances.getValue();

                WordTree wordTree = wordTrees.containsKey(fieldName) ? wordTrees.get(fieldName) : new WordTree(fieldName, hlPre, hlPost);
                wordTree.addSnippets(snippets);
                wordTrees.put(fieldName, wordTree);
            }
        }

        return getNonEmptyWordTrees(wordTrees);
    }

    private void printWordTrees(List<WordTree> wordTrees, StringWriter writer) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);
        g.writeStartObject();

        for(WordTree tree : wordTrees) {
            tree.combine();
            tree.printTree(g);
        }

        g.writeEndObject();
        g.close();
        g.flush();
    }

    @RequestMapping(value="/snippets/firstpage", method = RequestMethod.POST)
    public ResponseEntity<String> analyze(@RequestBody String body) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        String hlResponse  = (String) map.get(HIGHLIGHT);
        JSONObject hlJson  = JSONObject.fromObject(hlResponse);
        String hlPre       = (String) hlJson.remove(HIGHLIGHT_PRE);
        String hlPost      = (String) hlJson.remove(HIGHLIGHT_POST);

        Map<String, HashMap<String, String>> highlighting = reconfigureHighlighting(hlJson);

        List<WordTree> wordTrees = new ArrayList<WordTree>();
        for(Map.Entry<String, HashMap<String, String>> entry : highlighting.entrySet()) {
            String field                      = entry.getKey();
            HashMap<String, String> hlSnippet = entry.getValue();

            WordTree wordTree = new WordTree(field, hlPre, hlPost);
            wordTree.addSnippets(new ArrayList<String>(hlSnippet.keySet()));
            wordTrees.add(wordTree);
        }

        StringWriter writer = new StringWriter();
        printWordTrees(wordTrees, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/snippets/all", method = RequestMethod.GET)
    public ResponseEntity<String> analyzeAll(@RequestParam(value = PARAM_QUERY, required = true) String queryStr,
                                             @RequestParam(value = PARAM_FQ, required = false) String fq,
                                             @RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                             @RequestParam(value = PARAM_ROWS, required = true) int rows,
                                             @RequestParam(value = PARAM_ANALYSIS_FIELD, required = false) String analysisField,
                                             @RequestParam(value = PARAM_HIGHLIGHTING, required = true) boolean useHighlighting)
            throws IOException {

        List<WordTree> wordTrees;
        StringWriter writer = new StringWriter();
        CollectionSchemaInfo info = collectionsConfig.getSchema(collection);
        CollectionConfig config = collectionsConfig.getCollectionConfig(collection);

        fq = composeFilterQuery(fq, info);

        if (useHighlighting) {
            QueryResponse rsp = searchService.findSearchResults(collection, queryStr, null, null, 0, rows, fq, null,
                                                                StringUtils.join(config.getWordTreeFields(), ","), true, info);

            wordTrees = getWordTreeFromSearchResults(rsp);
        } else {
            String field = info.getCorrespondingFacetFieldIfExists(analysisField);
            GroupResponse rsp = searchService.getGroupResponse(collection, queryStr, fq, 100,//ReportConstants.SOLR_ANALYSIS_ROWS_LIMIT,
                                                               field, field, Defaults.ANALYSIS_GROUP_LIMIT);
            wordTrees = getWordTreesFromGroupResults(rsp);
        }

        printWordTrees(wordTrees, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/wordtreetest", method = RequestMethod.GET)
    public ResponseEntity<String> test() throws IOException {

        WordTree tree = new WordTree("test", "<b>", "</b>");

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
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
