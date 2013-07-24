package model.search;

public class Suggestion {
    public String text;
    public long numFound;
    public double score;
    public String field;

    public Suggestion(String text, String field, long numFound, double score) {
        this.text = text;
        this.numFound = numFound;
        this.score = score;
        this.field = field;
    }

    public String getText() {
        return text;
    }

    public long getNumFound() {
        return numFound;
    }

    public double getScore() {
        return score;
    }

    public String getField() {
        return this.field;
    }
}
