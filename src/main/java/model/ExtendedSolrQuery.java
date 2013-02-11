package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.Utils;
import org.apache.solr.client.solrj.SolrQuery;


public class ExtendedSolrQuery extends SolrQuery {
    
    public ExtendedSolrQuery() {
        super();
        this.add(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);
    }

    public ExtendedSolrQuery(String queryString) {
        this();
        this.setQuery(queryString);
    }

    public void setHighlightFields(String fields) {
        this.add(Constants.SOLR_HIGHLIGHT_FIELDLIST_PARAM, fields);
    }

    public void setHighlightQuery(String queryString, String fq) {
        this.add(Constants.SOLR_HIGHLIGHT_QUERY_PARAM, queryString + (fq != null ? " " + fq : ""));
    }

    public void setHighlightDefaults() {
        this.setHighlight(true);
        this.setHighlightSimplePre(Constants.SOLR_HIGHLIGHT_PRE_DEFAULT);
        this.setHighlightSimplePost(Constants.SOLR_HIGHLIGHT_POST_DEFAULT);
        this.setHighlightSnippets(Constants.SOLR_HIGHLIGHT_SNIPPETS_DEFAULT);
        this.setHighlightFragsize(Constants.SOLR_HIGHLIGHT_FRAGSIZE_DEFAULT);
    }

    public void setFilterQuery(String fq) {
        if (!Utils.nullOrEmpty(fq)) {
            this.addFilterQuery(fq);
        }
    }

    public void setViewFields(String viewFields) {
        if (!Utils.nullOrEmpty(viewFields)) {
            this.setFields(viewFields);
        }
    }

    public void setGroupField(String field) {
        this.add(Constants.SOLR_GROUP_FIELD_PARAM, field);
    }

    public void setGroupSort(String sort) {
        this.add(Constants.SOLR_GROUP_SORT_PARAM, sort);
    }

    public void setGroupSortDefault() {
        this.add(Constants.SOLR_GROUP_SORT_PARAM, Constants.SOLR_GROUP_SORT_DEFAULT);
    }

    public void setGroup(boolean groupsOn) {
        this.add(Constants.SOLR_GROUP_PARAM, groupsOn + "");
    }

    public void setGroupingDefaults(String groupField) {
        setGroup(true);
        setGroupSortDefault();
        setGroupField(groupField);
    }
}

