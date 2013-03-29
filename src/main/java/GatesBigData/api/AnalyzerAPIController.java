package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.JsonParsingUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.ExtendedSolrQuery;
import model.SolrCollectionSchemaInfo;
import model.WordTree;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/analyze")
public class AnalyzerAPIController extends APIController {

    private static final String ID_TO_TITLE_MAP_KEY = "idToTitleMap";
    private static final Logger logger = Logger.getLogger(AnalyzerAPIController.class);

    private SearchService searchService;

    @Autowired
    public AnalyzerAPIController(SearchService searchService) {
        this.searchService = searchService;
    }

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

    private HashMap<String, String> getIDtoTitleMap(SolrDocumentList docs) {
        HashMap<String, String> idToTitleMap = new HashMap<String, String>();
        for(SolrDocument doc : docs) {
            String id = SolrUtils.getFieldStringValue(doc, Constants.SOLR_ID_FIELD_NAME, "");
            String title = SolrUtils.getFieldStringValue(doc, Constants.SOLR_TITLE_FIELD_NAME, "");
            if (!Utils.nullOrEmpty(id)) {
                idToTitleMap.put(id, Utils.nullOrEmpty(title) ? id : title);
            }
        }
        return idToTitleMap;
    }

    public List<WordTree> getWordTreesFromGroupResults(GroupResponse rsp) {

        HashMap<String, WordTree> wordTrees = new HashMap<String, WordTree>();
        if (rsp != null) {
            for(GroupCommand command : rsp.getValues()) {
                for(Group group : command.getValues()) {
                    String snippet = group.getGroupValue();
                    if (Utils.nullOrEmpty(snippet)) continue;

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

    public List<WordTree> getWordTreesFromSearchResults(QueryResponse rsp, Set<String> fieldSet) {
        String hlPre  = SolrUtils.getResponseHeaderParam(rsp, Constants.SOLR_HIGHLIGHT_PRE_PARAM);
        String hlPost = SolrUtils.getResponseHeaderParam(rsp, Constants.SOLR_HIGHLIGHT_POST_PARAM);
        Map<String, Map<String, List<String>>> highlighting = rsp.getHighlighting();

        HashMap<String, WordTree> wordTrees = new HashMap<String, WordTree>();
        for(String field : fieldSet) {
            if (field.equals(Constants.SOLR_ID_FIELD_NAME)) continue;
            wordTrees.put(field, new WordTree(field, hlPre, hlPost));
        }

        for(SolrDocument doc : rsp.getResults()) {
            String id = (String) doc.getFieldValue(Constants.SOLR_ID_FIELD_NAME);
            //String title = (String) doc.getFieldValue(Constants.SOLR_TITLE_FIELD_NAME);

            for(Map.Entry<String, List<String>> fieldNameToInstances : highlighting.get(id).entrySet()) {
                String fieldName       = fieldNameToInstances.getKey();
                List<String> instances = fieldNameToInstances.getValue();
                wordTrees.get(fieldName).addSnippets(instances);
            }
        }

        return getNonEmptyWordTrees(wordTrees);
    }

    private void printWordTrees(List<WordTree> wordTrees, StringWriter writer) {
        JsonFactory f = new JsonFactory();

        try {
            JsonGenerator g = f.createJsonGenerator(writer);
            g.writeStartObject();

            for(WordTree tree : wordTrees) {
                tree.combine();
                tree.printTree(g);
            }

            g.writeEndObject();
            g.close();
            g.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @RequestMapping(value="/snippets/firstpage", method = RequestMethod.POST)
    public ResponseEntity<String> analyze(@RequestBody String body) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        String hlResponse                   = (String) map.get(Constants.SOLR_HIGHLIGHT_PARAM);
        JSONObject hlJson                   = JSONObject.fromObject(hlResponse);
        String hlPre                        = (String) hlJson.remove(Constants.SOLR_HIGHLIGHT_PRE_PARAM);
        String hlPost                       = (String) hlJson.remove(Constants.SOLR_HIGHLIGHT_POST_PARAM);
        JSONObject idToTitleJSONObj         = (JSONObject) hlJson.remove(ID_TO_TITLE_MAP_KEY);
        HashMap<String,String> idToTitleMap = JsonParsingUtils.convertJSONObjectToHashMap(idToTitleJSONObj);

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
                                             @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                             @RequestParam(value = PARAM_ROWS, required = true) int rows,
                                             @RequestParam(value = PARAM_FIELDS, required = true) String fields,
                                             @RequestParam(value = PARAM_ANALYSIS_FIELD, required = false) String analysisField,
                                             @RequestParam(value = PARAM_HIGHLIGHTING, required = true) boolean useHighlighting,
                                             HttpServletRequest request)
            throws IOException {

        HttpSession session = request.getSession();

        Set<String> fieldSet = new HashSet<String>(Arrays.asList(fields.split(Constants.DEFAULT_DELIMETER)));
        fieldSet.addAll(Arrays.asList(Constants.SOLR_TITLE_FIELD_NAME, Constants.SOLR_ID_FIELD_NAME));
        fields = StringUtils.join(fieldSet, Constants.DEFAULT_DELIMETER);

        List<WordTree> wordTrees;

        if (useHighlighting) {
            ExtendedSolrQuery query = searchService.buildHighlightQuery(coreName, queryStr, fq, Constants.SOLR_SORT_FIELD_DEFAULT,
                                                                        Constants.SOLR_SORT_ORDER_DEFAULT, 0, rows, null, fields);
            QueryResponse rsp = searchService.execQuery(query, coreName);
            wordTrees = getWordTreesFromSearchResults(rsp, fieldSet);
        } else {
            SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, session);
            GroupResponse rsp = searchService.getGroupResponse(coreName, queryStr, fq, Constants.SOLR_ANALYSIS_ROWS_LIMIT,
                    fields, schemaInfo.getCorrespondingFacetFieldIfExists(analysisField), Constants.SOLR_ANALYSIS_GROUP_LIMIT);
            wordTrees = getWordTreesFromGroupResults(rsp);
        }

        StringWriter writer = new StringWriter();
        printWordTrees(wordTrees, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
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
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
