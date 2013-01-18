package model;

import LucidWorksApp.utils.SolrUtils;
import LucidWorksApp.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordTree {

    private WordNode prefixQueryRoot;
    private WordNode suffixQueryRoot;
    private String hlPre;
    private String hlPost;
    private Pattern hlPattern;
    private String field;

    private Map<String, Number> queriesAndCts = new HashMap<String, Number>();
    private List<String> queries = new ArrayList<String>();
    private List<String> lhs = new ArrayList<String>();
    private List<String> rhs = new ArrayList<String>();

    public static final String COUNT_KEY        = "count";
    public static final String NAME_KEY         = "name";
    public static final String ALT_QUERIES_KEY  = "alternate_queries";
    public static final String CHILDREN_KEY     = "children";
    public static final String SUFFIX_KEY       = "suffix";
    public static final String PREFIX_KEY       = "prefix";
    public static final String FIELD_KEY        = "field";
    public static final String TREE_TYPE_KEY    = "treetype";

    private Logger logger = Logger.getLogger(WordTree.class);

    public WordTree(String field, String hlPre, String hlPost) {
        this.field = field;
        this.hlPre = hlPre;
        this.hlPost = hlPost;
        this.hlPattern = Pattern.compile("([^\\s]*)" + this.hlPre + "(.*?)" + this.hlPost + "([^\\s]*)");
    }

    private String[] splitContent(String content) {
        return content.split(" ");
    }

    private List<String> getSnippetWords(String snippet) {
        snippet = Utils.replaceHTMLAmpersands(SolrUtils.stripHighlightHtml(snippet, hlPre, hlPost));
        return Arrays.asList(splitContent(snippet));
    }

    public void addSnippet(String snippet) {
        Matcher m = hlPattern.matcher(snippet);
        List<String> words = getSnippetWords(snippet);

        while (m.find()) {
            String query = m.group(2);

            int queryCt = queriesAndCts.containsKey(query) ? queriesAndCts.get(query).intValue() + 1 : 0;
            queriesAndCts.put(query, queryCt);

            String wholeWord = m.group(1) + m.group(2) + m.group(3);

            //int index = words.indexOf(wholeWord);
            //lhs.add(StringUtils.join(words.subList(0, index), ","));
            //rhs.add(StringUtils.join(words.subList(index+1, words.size()), ","));
            addSuffixSnippet(query, wholeWord, words);
            addPrefixSnippet(query, wholeWord, words);
        }
    }

    private void addSuffixSnippet(String query, String wholeWord, List<String> words) {
        if (suffixQueryRoot == null) {
            suffixQueryRoot = new WordNode(query, wholeWord);
        } else {
            suffixQueryRoot.increment(query, wholeWord);
        }

        addSnippetWords(suffixQueryRoot, words.subList(words.indexOf(wholeWord) + 1, words.size()));
    }

    private void addPrefixSnippet(String query, String wholeWord, List<String> words) {
        if (prefixQueryRoot == null) {
            prefixQueryRoot = new WordNode(query, wholeWord);
        } else {
            prefixQueryRoot.increment(query, wholeWord);
        }
        Collections.reverse(words);
        addSnippetWords(prefixQueryRoot, words.subList(words.indexOf(wholeWord) + 1, words.size()));
    }

    private void addSnippetWords(WordNode curr, List<String> words) {
        for(String word : words) {
            curr = curr.addChild(word);
        }
    }

    public void combine() {
        combineTree(suffixQueryRoot);
        combineTree(prefixQueryRoot);
    }

    private void combineTree(WordNode curr) {
        if (curr.hasChildren()) {
            if (curr.nChildren() == 1) {
                WordNode child = curr.getChildren().get(0);
                if (child.getCount() == curr.getCount()) {
                    curr.combine(child);
                }
            }

            for(WordNode child : curr.getChildren()) {
                combineTree(child);
            }
        }
    }

    public void printTree(StringWriter writer) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);
        printTree(g);
        g.close();
        g.flush();
    }

    public void printTree(JsonGenerator g) throws IOException {
        g.writeObjectFieldStart(this.field);

        queriesAndCts = Utils.sortByValue(queriesAndCts);
        for(Map.Entry<String, Number> entry : queriesAndCts.entrySet()) {
            queries.add(entry.getKey() + " (" + entry.getValue() + ")");
        }

        //printStrings(queries, "queries", g);
        //printStrings(lhs, "left", g);
        //printStrings(rhs, "right", g);

        printTree(suffixQueryRoot, g);
        printTree(prefixQueryRoot, g);

        g.writeEndObject();
    }

    private void printStrings(Collection<String> strings, String fieldName, JsonGenerator g) throws IOException {
        g.writeArrayFieldStart(fieldName);
        for(String s : strings) {
            g.writeString(s);
        }
        g.writeEndArray();
    }

    public void printTree(WordNode curr, JsonGenerator g) throws IOException {
        if (curr == suffixQueryRoot) {
            g.writeObjectFieldStart(SUFFIX_KEY);
        } else if (curr == prefixQueryRoot) {
            g.writeObjectFieldStart(PREFIX_KEY);
        } else {
            g.writeStartObject();
        }

        Utils.writeValueByType(NAME_KEY, curr.getWord(), g);
        Utils.writeValueByType(COUNT_KEY, curr.getCount(), g);

        if (curr.isQuery()) {
            HashMap<String, Integer> altQueries = curr.getAltQueries();
            g.writeArrayFieldStart(ALT_QUERIES_KEY);
            for(Map.Entry<String, Integer> entry : altQueries.entrySet()) {
                g.writeStartObject();
                Utils.writeValueByType(NAME_KEY, entry.getKey(), g);
                Utils.writeValueByType(COUNT_KEY, entry.getValue(), g);
                g.writeEndObject();
            }
            g.writeEndArray();
        }
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
}
