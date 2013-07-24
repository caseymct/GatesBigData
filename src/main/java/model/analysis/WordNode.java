package model.analysis;

import GatesBigData.utils.Utils;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class WordNode {
    private long count;
    private String word;
    private String sentence;
    private boolean isPrefix;

    private List<WordNode> children = new ArrayList<WordNode>();

    public WordNode(String word, String sentence, long count, boolean isPrefix) {
        this.word     = word;
        this.count    = count;
        this.sentence = sentence;
        this.isPrefix = isPrefix;
    }

    public WordNode(String word, long count, boolean isPrefix) {
        this(word, word, count, isPrefix);
    }

    public void setSentence(String sentence) {
        this.sentence = sentence;
    }

    public String getSentence() {
        return this.sentence;
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

    public void combine(WordNode child) {
        this.word = this.isPrefix ? child.word + " " + this.word : this.word + " " + child.word;
        this.sentence = child.sentence;
        this.children = child.getChildren();
    }

    public void update(long count) {
        this.increment(count);
    }

    public WordNode addChild(String word, long count, String sentence) {
        for(WordNode child : this.children) {
            if (child.getWord().equals(word)) {
                child.update(count);
                return child;
            }
        }
        WordNode child = new WordNode(word, sentence, count, this.isPrefix);
        children.add(child);
        return child;
    }
}
