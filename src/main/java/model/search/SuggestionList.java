package model.search;

import GatesBigData.utils.JSONUtils;
import net.sf.json.JSONArray;

import java.util.*;

public class SuggestionList implements Iterable<Suggestion> {
    List<Suggestion> suggestions = new ArrayList<Suggestion>();
    private int count = 0;

    public SuggestionList() {
    }

    public void add(String text, String field, long numFound, double score) {
        this.suggestions.add(new Suggestion(text, field, numFound, score));
    }

    public void concat(SuggestionList newList) {
        for(Suggestion suggestion : newList) {
            suggestions.add(suggestion);
        }
    }

    private void sort() {
        Collections.sort(suggestions, new Comparator<Suggestion>() {
            public int compare(Suggestion s1, Suggestion s2) {
                double x1 = s1.getScore(), x2 = s2.getScore();
                if (x1 > x2) return -1;
                if (x1 < x2) return 1;

                long nf1 = s1.getNumFound(), nf2 = s2.getNumFound();
                if (nf1 > nf2) return -1;
                if (nf1 < nf2) return 1;
                return 0;
            }});
    }

    private String formatSuggestionText(Suggestion s) {
        return "<b>" + s.getText() + "</b> <i>" + s.getField() + "</i> " + " (" + s.getNumFound() + ")";
    }

    public List<String> getFormattedSuggestionList() {
        List<String> formattedSuggestions = new ArrayList<String>();
        for(Suggestion s : suggestions) {
            formattedSuggestions.add(formatSuggestionText(s));
        }
        return formattedSuggestions;
    }

    public List<String> getSortedFormattedSuggestionList() {
        sort();
        return getFormattedSuggestionList();
    }

    public JSONArray getSortedFormattedSuggestionJSONArray() {
        return JSONUtils.convertCollectionToJSONArray(getSortedFormattedSuggestionList());
    }

    public JSONArray getFormattedSuggestionJSONArray() {
        return JSONUtils.convertCollectionToJSONArray(getFormattedSuggestionList());
    }

    public int size() {
        return suggestions.size();
    }

    public boolean hasNext() {
        return count < suggestions.size();
    }

    public Suggestion next() {
        if (count == suggestions.size())
            throw new NoSuchElementException();

        count++;
        return suggestions.get(count);
    }

    public Iterator<Suggestion> iterator() {
        count = 0;
        return suggestions.iterator();
    }
}

