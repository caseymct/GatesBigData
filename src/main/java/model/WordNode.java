package model;


import LucidWorksApp.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WordNode {
    private int count;
    private String word;
    private boolean isQuery;
    private HashMap<String, Integer> altQueries = new HashMap<String, Integer>();
    private List<WordNode> children = new ArrayList<WordNode>();

    public WordNode(String word) {
        this.word = word;
        this.count = 1;
        this.isQuery = false;
    }

    public WordNode(String query, String altQuery) {
        this.word = query;
        this.count = 1;
        this.isQuery = true;
        addAltQuery(altQuery);
    }

    public String getWord() {
        return word;
    }

    public int getCount() {
        return count;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public HashMap<String, Integer> getAltQueries() {
        return altQueries;
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

    public void increment(String query, String altQuery) {
        this.count++;
        addAltQuery(altQuery);
    }

    public void combine(WordNode child) {
        this.word += " " + child.word;
        this.children = child.children;
    }

    public void addAltQuery(String altQuery) {
        int aqCount = 1 + (altQueries.containsKey(altQuery) ? altQueries.get(altQuery) : 0);
        altQueries.put(altQuery, aqCount);
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
