package model;

import GatesBigData.utils.Utils;
import java.util.*;

public class WordNode {
    private int count;
    private String word;
    private Set<String> solrIds;

    private List<WordNode> children = new ArrayList<WordNode>();

    public WordNode(String word) {
        this.word = word;
        this.count = 1;
        this.solrIds = new HashSet<String>();
    }

    public WordNode(String query, String solrId) {
        this(query);
        addSolrId(solrId);
    }

    public void addSolrId(String solrId) {
        this.solrIds.add(solrId);
    }

    public Set<String> getSolrIds() {
        return this.solrIds;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public List<WordNode> getChildren() {
        return this.children;
    }

    public boolean hasChildren() {
        return !Utils.nullOrEmpty(this.children);
    }

    public int nChildren() {
        return this.children.size();
    }

    public void increment() {
        this.count++;
    }

    public void combine(WordNode child, boolean isPrefix) {
        this.word = isPrefix ? child.word + " " + this.word : this.word + " " + child.word;
        this.children = child.children;
        this.solrIds.addAll(child.solrIds);
    }

    public WordNode addChild(String word) {
        for(WordNode child : this.children) {
            if (child.getWord().equals(word)) {
                child.increment();
                return child;
            }
        }
        WordNode child = new WordNode(word);
        children.add(child);
        return child;
    }
}
