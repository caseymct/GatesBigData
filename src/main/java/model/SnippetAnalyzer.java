package model;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.SolrUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.*;

public class SnippetAnalyzer {

    private List<String> snippetContentStrings = new ArrayList<String>();
    private List<Snippet> snippets = new ArrayList<Snippet>();
    private HashMap<String, HashMap<String, Double>> queriesToWordsMap = new HashMap<String, HashMap<String, Double>>();
    private Map<String, List<String>> highlighting = new HashMap<String, List<String>>();
    private String hlPre;
    private String hlPost;

    private static final Logger logger = Logger.getLogger(SnippetAnalyzer.class);

    // highlighting: {
    //      key {
    //          field : [ values ]
    //          field : [ values ]
    //      }, ...
    public SnippetAnalyzer(String highlightResponse) {
        JSONObject highlightJson = JSONObject.fromObject(highlightResponse);
        hlPre  = (String) highlightJson.remove(Constants.SOLR_HIGHLIGHT_PRE_PARAM);
        hlPost = (String) highlightJson.remove(Constants.SOLR_HIGHLIGHT_POST_PARAM);

        for(Object keyObj : highlightJson.names()) {
            JSONObject docObj = highlightJson.getJSONObject(keyObj.toString());

            for(Object docKeyObj : docObj.names()) {
                String field = docKeyObj.toString();
                List<String> snippetStrings = JsonParsingUtils.convertJSONArrayToStringList(docObj.getJSONArray(field));

                if (highlighting.containsKey(field)) {
                    snippetStrings.addAll(highlighting.get(field));
                }
                highlighting.put(field, snippetStrings);
            }
        }
    }

    public Map<String, List<String>> getHighlighting() {
        return highlighting;
    }

    public String getHlPre() {
        return hlPre;
    }

    public String getHlPost() {
        return hlPost;
    }

    private void init() {
        for(String snippetContentString : snippetContentStrings) {
            Snippet snippet = new Snippet(snippetContentString, hlPre, hlPost);
            snippets.add(snippet);

            for(Map.Entry<String, HashMap<String, Double>> entry : snippet.getQueriesToWordsMap().entrySet()) {
                String query = entry.getKey();
                HashMap<String, Double> snippetBoostMap = entry.getValue();
                HashMap<String, Double> corpusBoostMap = queriesToWordsMap.containsKey(query) ? queriesToWordsMap.get(query) :
                                                    new HashMap<String, Double>();

                for(Map.Entry<String, Double> snippetBoostEntry : snippetBoostMap.entrySet()) {
                    String word = snippetBoostEntry.getKey();
                    double value = snippetBoostEntry.getValue();
                    corpusBoostMap.put(word, SolrUtils.getDoubleValueIfExists(corpusBoostMap, word) + value);
                }
                queriesToWordsMap.put(query, corpusBoostMap);
            }
        }
    }

    class ValueComparator implements Comparator<String> {
        Map<String, Double> base;
        public ValueComparator(Map<String, Double> base) {
            this.base = base;
        }

        public int compare(String a, String b) {
            return (base.get(a) >= base.get(b)) ? -1 : 1;
        }
    }


    public HashMap<String, TreeMap<String, Double>> getQueriesToWordsMap() {
        HashMap<String, TreeMap<String, Double>> sortedMap = new HashMap<String, TreeMap<String, Double>>();

        for(Map.Entry<String, HashMap<String, Double>> queriesToWordsMapEntry : queriesToWordsMap.entrySet()) {
            ValueComparator bvc = new ValueComparator(queriesToWordsMapEntry.getValue());
            TreeMap<String, Double> wordBoostMap = new TreeMap<String,Double>(bvc);
            wordBoostMap.putAll(queriesToWordsMapEntry.getValue());
            sortedMap.put(queriesToWordsMapEntry.getKey(), wordBoostMap);
        }

        return sortedMap;
    }

    public void writeMapToJson(JsonGenerator g) {
        try {
            g.writeObjectFieldStart("field");

            for(Map.Entry<String, TreeMap<String, Double>> entry : getQueriesToWordsMap().entrySet()) {
                g.writeArrayFieldStart(entry.getKey());

                for(Map.Entry<String, Double> boostEntry : entry.getValue().entrySet()) {
                    g.writeStartObject();
                    Utils.writeValueByType("word", boostEntry.getKey(), g);
                    Utils.writeValueByType("score", boostEntry.getValue(), g);
                    g.writeEndObject();
                }
                g.writeEndArray();
            }
            g.writeEndObject();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
