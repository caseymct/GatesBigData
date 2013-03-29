package model;

import GatesBigData.utils.Utils;
import java.util.*;

public class WordNode {
    private long count;
    private String word;
    private Set<String> groupQueries;

    private List<WordNode> children = new ArrayList<WordNode>();

    public WordNode(String word, long count) {
        this.word         = word;
        this.count        = count;
        this.groupQueries = new HashSet<String>();
    }

    public WordNode(String word, long count, List<String> groupQueries) {
        this(word, count);
        this.groupQueries.addAll(groupQueries);
    }

    public WordNode(String word, long count, String groupQuery) {
        this(word, count, Arrays.asList(groupQuery));
    }

    public void addGroupQuery(String groupQuery) {
        this.groupQueries.add(groupQuery);
    }

    public void addGroupQueries(List<String> groupQueries) {
        this.groupQueries.addAll(groupQueries);
    }

    public Set<String> getGroupQueries() {
        return this.groupQueries;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
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

    public void increment(long count) {
        this.count += count;
    }

    public void increment() {
        increment(1);
    }

    public void combine(WordNode child, boolean isPrefix) {
        this.word = isPrefix ? child.word + " " + this.word : this.word + " " + child.word;
        this.children = child.getChildren();
        this.update(0, child.getGroupQueries());
    }

    public void update(long count, List<String> groupQueries) {
        this.increment(count);
        this.addGroupQueries(groupQueries);
    }

    public void update(long count, String groupQuery) {
        update(count, Arrays.asList(groupQuery));
    }

    public void update(long count, Set<String> groupQueries) {
        update(count, new ArrayList<String>(groupQueries));
    }

    public WordNode addChild(String word, long count, String groupQuery) {
        for(WordNode child : this.children) {
            if (child.getWord().equals(word)) {
                child.update(count, groupQuery);
                return child;
            }
        }
        WordNode child = new WordNode(word, count, groupQuery);
        children.add(child);
        return child;
    }
}
