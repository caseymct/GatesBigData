package model;

import LucidWorksApp.utils.SolrUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Snippet {
    private String snippetContent;
    private String hlPre;
    private String hlPost;
    private Pattern hlPattern;

    private List<String> snippetWords;
    private HashMap<String, HashMap<String, Double>> queriesToWordsMap = new HashMap<String, HashMap<String, Double>>();

    public Snippet(String snippetContent, String hlPre, String hlPost) {
        this.snippetContent = snippetContent;
        this.hlPre  = hlPre;
        this.hlPost = hlPost;
        this.hlPattern = Pattern.compile("([^\\s]*)" + hlPre + "(.*?)" + hlPost + "([^\\s]*)");
        init();
    }

    public HashMap<String, HashMap<String, Double>> getQueriesToWordsMap() {
        return queriesToWordsMap;
    }

    private String[] splitContent(String content) {
        return content.split(" ");
    }

    public void init() {
        Matcher m = hlPattern.matcher(snippetContent);
        this.snippetWords = Arrays.asList(splitContent(SolrUtils.stripHighlightHtml(snippetContent, hlPre, hlPost)));

        while (m.find()) {
            String query = m.group(2);
            String wholeWord = m.group(1) + m.group(2) + m.group(3);
            setWordBoost(query, wholeWord);
        }
    }

    public float gaussianValue(float x) {
        return (float) Math.exp(-4.0*x*x);
    }

    public void setWordBoost(String query, String wholeWord) {
        //how to handle multi-word queries?
        HashMap<String, Double> wordBoost = queriesToWordsMap.containsKey(query) ? queriesToWordsMap.get(query) :
                new HashMap<String, Double>();

        int i, nSnippetWords = snippetWords.size();
        int queryLoc = snippetWords.indexOf(wholeWord);
        int splitSize = Math.max(nSnippetWords - queryLoc, queryLoc);

        for(i = 0; i < nSnippetWords; i++) {
            String word = snippetWords.get(i);
            float boost = 1.0f;
            if (queryLoc >= 0) {
                float x = (i < queryLoc) ? (queryLoc - i)/(splitSize + 0.0f) : i/(splitSize + 0.0f);
                boost = gaussianValue(x);
            }
            wordBoost.put(word, SolrUtils.getDoubleValueIfExists(wordBoost, word) + boost);
        }
        queriesToWordsMap.put(query, wordBoost);
    }
}
