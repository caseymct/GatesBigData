package model;


public class Suggestion {
    public String text;
    public int numFound;
    public double score;
    public String field;

    public Suggestion(String text, String field, int numFound, double score) {
        this.text = text;
        this.numFound = numFound;
        this.score = score;
        this.field = field;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getNumFound() {
        return numFound;
    }

    public void setNumFound(int numFound) {
        this.numFound = numFound;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getField() {
        return this.field;
    }
}
