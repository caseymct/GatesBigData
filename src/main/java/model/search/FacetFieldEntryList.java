package model.search;

import GatesBigData.utils.SolrUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FacetFieldEntryList implements Iterable<FacetFieldEntry> {
    private List<String> names;
    private List<FacetFieldEntry> facetFieldList;
    private int count = 0;

    public FacetFieldEntryList() {
        this.names = new ArrayList<String>();
        this.facetFieldList = new ArrayList<FacetFieldEntry>();
    }

    public void add(String name, String type, boolean multiValued) {
        if (names.contains(name)) {
            return;
        }

        facetFieldList.add(new FacetFieldEntry(name, type, multiValued));
        names.add(name);
    }

    public void add(String name, String type, String schema) {
        add(name, type, SolrUtils.schemaStringIndicatesMultiValued(schema));
    }

    public void intersect(List<String> names, FacetFieldEntryList list) {
        for(String facetField : names) {
            if (!this.containsKey(facetField) && list.containsKey(facetField)) {
                this.add(list.get(facetField));
            }
        }
    }

    public void add(FacetFieldEntry entry) {
        facetFieldList.add(entry);
        names.add(entry.getFieldName());
    }

    public void addAll(FacetFieldEntryList facetFieldEntryList) {
        for(FacetFieldEntry entry : facetFieldEntryList) {
            String name = entry.getFieldName();
            if (!this.names.contains(name)) {
                this.facetFieldList.add(entry);
                this.names.add(name);
            }
        }
    }

    public int size() {
        return facetFieldList.size();
    }

    public List<String> getNames() {
        return this.names;
    }

    public boolean isFieldMultivalued(String name) {
        int index = names.indexOf(name);
        return facetFieldList.get(index).isMultiValued();
    }

    public boolean containsKey(String name) {
        return names.indexOf(name) > -1;
    }

    public FacetFieldEntry get(String name) {
        int idx = names.indexOf(name);
        return idx > -1 ? facetFieldList.get(idx) : null;
    }

    public FacetFieldEntry get(int index) {
        return facetFieldList.get(index);
    }

    public void remove(String name) {
        this.remove(names.indexOf(name));
    }

    public void remove(int index) {
        facetFieldList.remove(index);
        names.remove(index);
    }

    public boolean hasNext() {
        return count < facetFieldList.size();
    }

    public FacetFieldEntry next() {
        if (count == facetFieldList.size())
            throw new NoSuchElementException();

        count++;
        return facetFieldList.get(count);
    }

    public Iterator<FacetFieldEntry> iterator() {
        return facetFieldList.iterator();
    }
}
