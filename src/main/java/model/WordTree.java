package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordTree {

    private WordNode prefixQueryRoot;
    private WordNode suffixQueryRoot;
    private String markPatternStart;
    private String markPatternEnd;
    private Pattern markPattern;
    private String field;
    private HashMap<String, String> solrIDToTitleMap;

    private HashMap<String, Integer> altQueries = new HashMap<String, Integer>();

    public static final String MARK_START_DEFAULT = "<CENTERWORD>";
    public static final String MARK_END_DEFAULT   = "</CENTERWORD>";
    public static final String COUNT_KEY          = "count";
    public static final String NAME_KEY           = "name";
    public static final String SOLR_IDS_KEY       = "solr_ids";
    public static final String GROUP_QUERIES_KEY  = "group_queries";
    public static final String ALT_QUERIES_KEY    = "alternate_queries";
    public static final String CHILDREN_KEY       = "children";
    public static final String SUFFIX_KEY         = "suffix";
    public static final String PREFIX_KEY         = "prefix";
    public static final List<String> EMPTY_LIST   = new ArrayList<String>();

    private Logger logger = Logger.getLogger(WordTree.class);

    public WordTree(String field, String markPatternStart, String markPatternEnd, HashMap<String,String> solrIDToTitleMap) {
        this.field = field;
        this.solrIDToTitleMap = solrIDToTitleMap;
        this.markPatternStart = markPatternStart;
        this.markPatternEnd = markPatternEnd;
        setMarkPattern();
    }

    public WordTree(String field, String markPatternStart, String markPatternEnd) {
        this(field, markPatternStart, markPatternEnd, new HashMap<String, String>());
    }

    public WordTree(String field) {
        this(field, MARK_START_DEFAULT, MARK_END_DEFAULT, new HashMap<String, String>());
    }

    private void setMarkPattern() {
        this.markPattern = Pattern.compile("([^\\s]*)" + this.markPatternStart + "(.*?)" + this.markPatternEnd + "([^\\s]*)");
    }

    private String[] splitContent(String content) {
        return content.split(" ");
    }

    private List<String> getSnippetWords(String snippet) {
        snippet = Utils.replaceHTMLAmpersands(SolrUtils.stripHighlightHtml(snippet, markPatternStart, markPatternEnd));
        return Arrays.asList(splitContent(snippet));
    }

    public void addAltQuery(String altQuery) {
        int aqCount = 1 + (altQueries.containsKey(altQuery) ? altQueries.get(altQuery) : 0);
        altQueries.put(altQuery, aqCount);
    }

    private void updateSolrIdToTitleMap(String id, String title) {
        if (!solrIDToTitleMap.containsKey(id)) {
            solrIDToTitleMap.put(id, title);
        }
    }

    public void addSnippets(HashMap<String, String> highlightStringToIdMap) {
        for(Map.Entry<String, String> entry : highlightStringToIdMap.entrySet()) {
            addSnippet(entry.getKey(), entry.getValue());
        }
    }

    public void addSnippets(List<String> snippets, String id, String title) {
        updateSolrIdToTitleMap(id, title);
        for(String snippet : snippets) {
            addSnippet(snippet, id);
        }
    }

    public void addSnippet(String snippet, String id, String title) {
        updateSolrIdToTitleMap(id, title);
        addSnippet(snippet, id);
    }

    public void addGroupQuerySnippet(String snippet, long count) {
        //Mark first word?
        List<String> words = getSnippetWords(snippet);
        String query = words.get(0);

        suffixQueryRoot = addSnippetToWordNode(suffixQueryRoot, query, words.subList(1, words.size()), snippet, count);
        prefixQueryRoot = addSnippetToWordNode(prefixQueryRoot, query, EMPTY_LIST, snippet, count);
    }

    public void addSnippet(String snippet, String id) {
        Matcher m = markPattern.matcher(snippet);
        List<String> words = getSnippetWords(snippet);
        List<String> ids = Arrays.asList(id);

        while (m.find()) {
            String query = m.group(2);

            String wholeWord = m.group(1) + m.group(2) + m.group(3);
            addAltQuery(wholeWord);

            int startIndex = words.indexOf(wholeWord) + 1, endIndex = words.size();
            suffixQueryRoot = addSnippetToWordNode(suffixQueryRoot, query, words.subList(startIndex, endIndex), snippet, 1);

            Collections.reverse(words);
            startIndex = words.indexOf(wholeWord) + 1;
            prefixQueryRoot = addSnippetToWordNode(prefixQueryRoot, query, words.subList(startIndex, endIndex), snippet, 1);
        }
    }

    private WordNode addSnippetToWordNode(WordNode wordNode, String query, List<String> words, String groupQuery, long count) {
        if (wordNode == null) {
            wordNode = new WordNode(query, count, groupQuery);
        } else {
            wordNode.update(count, groupQuery);
        }

        addSnippetWords(wordNode, words, groupQuery, count);
        return wordNode;
    }

    private void addSnippetWords(WordNode curr, List<String> words, String groupQuery, long count) {
        for(String word : words) {
            curr = curr.addChild(word, count, groupQuery);
        }
    }

    public void combine() {
        combineTree(suffixQueryRoot, false);
        combineTree(prefixQueryRoot, true);
        combineRootWords();
    }

    private void combineRootWords() {
        if (suffixQueryRoot == null || prefixQueryRoot == null) {
            return;
        }

        if (suffixQueryRoot.getCount() == prefixQueryRoot.getCount()) {
            List<String> combinedPrefixWords = Arrays.asList(prefixQueryRoot.getWord().split(" "));
            List<String> combinedSuffixWords = Arrays.asList(suffixQueryRoot.getWord().split(" "));
            int pEnd = combinedPrefixWords.size() - 1, sEnd = combinedSuffixWords.size() - 1;

            String lastPrefixWord  = combinedPrefixWords.get(pEnd);
            String firstSuffixWord = combinedSuffixWords.get(0);

            String combinedWord = (pEnd >= 0) ? StringUtils.join(combinedPrefixWords.subList(0, pEnd), " ") : "";
            if (lastPrefixWord.toLowerCase().equals(firstSuffixWord.toLowerCase())) {
                combinedWord += " " + suffixQueryRoot.getWord();
            } else {
                combinedWord += " (" + lastPrefixWord + "/" + firstSuffixWord + ") ";
                if (sEnd > 1) {
                    combinedWord += StringUtils.join(combinedSuffixWords.subList(1, sEnd), " ");
                }
            }

            suffixQueryRoot.setWord(combinedWord);
            prefixQueryRoot.setWord(combinedWord);
        }
    }

    private void combineTree(WordNode curr, boolean isPrefix) {
        if (curr == null) {
            return;
        }

        if (curr.nChildren() == 1) {
            WordNode child = curr.getChildren().get(0);
            if (child.getCount() == curr.getCount()) {
                curr.combine(child, isPrefix);
                combineTree(curr, isPrefix);
            }
        }
        for(WordNode child : curr.getChildren()) {
            combineTree(child, isPrefix);
        }
    }

    public void printTree(JsonGenerator g) throws IOException {
        g.writeObjectFieldStart(this.field);

        printAltQueries(g);
        printTree(suffixQueryRoot, g);
        printTree(prefixQueryRoot, g);

        g.writeEndObject();
    }

    private void printAltQueries(JsonGenerator g) throws IOException {
        g.writeArrayFieldStart(ALT_QUERIES_KEY);
        for(Map.Entry<String, Integer> entry : altQueries.entrySet()) {
            g.writeStartObject();
            Utils.writeValueByType(NAME_KEY, entry.getKey(), g);
            Utils.writeValueByType(COUNT_KEY, entry.getValue(), g);
            g.writeEndObject();
        }
        g.writeEndArray();
    }

    private void printStrings(Collection<String> strings, String fieldName, JsonGenerator g) throws IOException {
        g.writeArrayFieldStart(fieldName);
        for(String s : strings) {
            g.writeString(s);
        }
        g.writeEndArray();
    }

    public void printTree(WordNode curr, JsonGenerator g) throws IOException {
        if (curr == null) {
            return;
        }

        if (curr == suffixQueryRoot) {
            g.writeObjectFieldStart(SUFFIX_KEY);
        } else if (curr == prefixQueryRoot) {
            g.writeObjectFieldStart(PREFIX_KEY);
        } else {
            g.writeStartObject();
        }

        Utils.writeValueByType(NAME_KEY, curr.getWord(), g);
        Utils.writeValueByType(COUNT_KEY, curr.getCount(), g);
        /*
        g.writeArrayFieldStart(SOLR_IDS_KEY);
        for(String solrId : curr.getSolrIds()) {
            g.writeStartObject();
            Utils.writeValueByType(Constants.SOLR_ID_FIELD_NAME, solrId, g);
            Utils.writeValueByType(Constants.SOLR_TITLE_FIELD_NAME, Utils.getObjectIfExists(solrIDToTitleMap, solrId, ""), g);
            g.writeEndObject();
        }
        g.writeEndArray();    */

        g.writeArrayFieldStart(GROUP_QUERIES_KEY);
        for(String groupQuery : curr.getGroupQueries()) {
            g.writeString(groupQuery);
        }
        g.writeEndArray();

        if (curr.hasChildren()) {
            g.writeArrayFieldStart(CHILDREN_KEY);
            for(WordNode child : curr.getChildren()) {
                printTree(child, g);
            }
            g.writeEndArray();
        }
        g.writeEndObject();
    }

    public void printTree(WordNode curr, String currSentence, StringWriter writer) throws IOException {
        currSentence += curr.getWord() + " (" + curr.getCount() + ") ";

        if (curr.hasChildren()) {
            for(WordNode child : curr.getChildren()) {
                printTree(child, currSentence, writer);
            }
        } else {
            writer.append(currSentence).append("\n");
        }
    }

    public boolean isEmpty() {
        return this.suffixQueryRoot == null && this.prefixQueryRoot == null;
    }
}
