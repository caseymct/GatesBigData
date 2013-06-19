package model;

import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
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

    private HashMap<String, Integer> altQueries = new HashMap<String, Integer>();

    public static final String MARK_START_DEFAULT = "<CENTERWORD>";
    public static final String MARK_END_DEFAULT   = "</CENTERWORD>";
    public static final String COUNT_KEY          = "count";
    public static final String NAME_KEY           = "name";
    public static final String SENTENCE_KEY       = "sentence";
    public static final String ALT_QUERIES_KEY    = "alternate_queries";
    public static final String CHILDREN_KEY       = "children";
    public static final String SUFFIX_KEY         = "suffix";
    public static final String PREFIX_KEY         = "prefix";
    public static final List<String> EMPTY_LIST   = new ArrayList<String>();

    private Logger logger = Logger.getLogger(WordTree.class);

    public WordTree(String field, String markPatternStart, String markPatternEnd) {
        this.field = field;
        this.markPatternStart = markPatternStart;
        this.markPatternEnd = markPatternEnd;
        setMarkPattern();
    }

    public WordTree(String field) {
        this(field, MARK_START_DEFAULT, MARK_END_DEFAULT);
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

    public void addSnippets(List<String> snippets) {
        for(String snippet : snippets) {
            addSnippet(snippet);
        }
    }

    public void addGroupQuerySnippet(String snippet, long count) {
        //Mark first word?
        List<String> words = getSnippetWords(snippet);
        String query = words.get(0);

        suffixQueryRoot = addSnippetToWordNode(suffixQueryRoot, query, words, count, false);
        prefixQueryRoot = addSnippetToWordNode(prefixQueryRoot, query, EMPTY_LIST, count, true);
    }

    public void addSnippet(String snippet) {
        Matcher m = markPattern.matcher(snippet);
        List<String> words = getSnippetWords(snippet);

        while (m.find()) {
            String query = m.group(2);

            String wholeWord = m.group(1) + m.group(2) + m.group(3);
            addAltQuery(wholeWord);

            int startIndex = words.indexOf(wholeWord), endIndex = words.size();
            suffixQueryRoot = addSnippetToWordNode(suffixQueryRoot, query, words.subList(startIndex, endIndex), 1, false);

            Collections.reverse(words);
            startIndex = words.indexOf(wholeWord);
            prefixQueryRoot = addSnippetToWordNode(prefixQueryRoot, query, words.subList(startIndex, endIndex), 1, true);
        }
    }

    private WordNode addSnippetToWordNode(WordNode wordNode, String query, List<String> words, long count, boolean isPrefix) {
        if (wordNode == null) {
            wordNode = new WordNode(query, count, isPrefix);
        } else {
            wordNode.update(count);
        }

        addSnippetWords(wordNode, words, count);
        return wordNode;
    }

    private void addSnippetWords(WordNode curr, List<String> words, long count) {
        for(int i = 1; i < words.size(); i++) {
            curr = curr.addChild(words.get(i), count, StringUtils.join(words.subList(0, i+1), " "));
        }
    }

    public void combine() {
        combineTree(suffixQueryRoot);
        combineTree(prefixQueryRoot);
        combineRootWords();
    }

    private String combineSentences(String prefix, String suffix) {
        List<String> prefixWords = Arrays.asList(prefix.split(" "));
        List<String> suffixWords = Arrays.asList(suffix.split(" "));

        int pEnd = prefixWords.size() - 1, sEnd = suffixWords.size();
        String lastPrefixWord  = prefixWords.get(pEnd);
        String firstSuffixWord = suffixWords.get(0);

        List<String> combined = new ArrayList<String>();
        if (pEnd >= 0) {
            combined.addAll(prefixWords.subList(0, pEnd));
        }

        if (lastPrefixWord.toLowerCase().equals(firstSuffixWord.toLowerCase())) {
            combined.addAll(suffixWords);
        } else {
            combined.add(lastPrefixWord.equals("") ? firstSuffixWord : "(" + lastPrefixWord + "/" + firstSuffixWord + ")");
            if (sEnd > 1) {
                combined.addAll(suffixWords.subList(1, sEnd));
            }
        }
        return StringUtils.join(combined, " ");
    }

    private void combineRootWords() {
        if (suffixQueryRoot == null || prefixQueryRoot == null) {
            return;
        }

        if (suffixQueryRoot.getCount() == prefixQueryRoot.getCount()) {
            String combinedWord = combineSentences(prefixQueryRoot.getWord(), suffixQueryRoot.getWord());
            String combinedSentence = combineSentences(prefixQueryRoot.getSentence(), suffixQueryRoot.getSentence());

            suffixQueryRoot.setSentence(combinedSentence);
            prefixQueryRoot.setSentence(combinedSentence);
            suffixQueryRoot.setWord(combinedWord);
            prefixQueryRoot.setWord(combinedWord);
        }
    }

    private void combineTree(WordNode curr) {
        if (curr == null) {
            return;
        }

        if (curr.nChildren() == 1) {
            WordNode child = curr.getChildren().get(0);
            if (child.getCount() == curr.getCount()) {
                curr.combine(child);
                combineTree(curr);
            }
        }
        for(WordNode child : curr.getChildren()) {
            combineTree(child);
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
            Utils.writeValueByType(ReportConstants.SOLR_FIELD_NAME_ID, solrId, g);
            Utils.writeValueByType(ReportConstants.SOLR_FIELD_NAME_TITLE, Utils.getObjectIfExists(solrIDToTitleMap, solrId, ""), g);
            g.writeEndObject();
        }
        g.writeEndArray();    */

        g.writeStringField(SENTENCE_KEY, curr.getSentence());

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
