package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtendedSolrQuery extends SolrQuery {

    public static final int GROUP_UNLIMITED = -1;

    public ExtendedSolrQuery() {
        super();
        this.add(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);
    }

    public ExtendedSolrQuery(String queryString) {
        this();
        this.setQuery(queryString);
    }

    public ExtendedSolrQuery(HashMap<String, String> queryParams) {
        this();
        this.setQuery(constructQueryString(queryParams, Constants.SOLR_DEFAULT_BOOLEAN_OP));
    }

    public ExtendedSolrQuery(HashMap<String, String> queryParams, String op) {
        this();
        this.setQuery(constructQueryString(queryParams, op));
    }

    private String constructQueryString(HashMap<String, String> queryParams, String op) {
        List<String> queryStringComponents = new ArrayList<String>();
        for(Map.Entry<String, String> entry : queryParams.entrySet()) {
            queryStringComponents.add(entry.getKey() + ":\"" + entry.getValue() + "\"");
        }

        return StringUtils.join(queryStringComponents, " " + op + " ");
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

    public void setFacetDefaults(int facetLimit) {
        this.setFacet(true);
        this.setFacetMissing(true);
        this.setFacetLimit(facetLimit);
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

    public void setGroupLimit(int limit) {
        this.add(Constants.SOLR_GROUP_LIMIT_PARAM, "" + limit);
    }

    public void setGroupLimitUnlimited() {
        this.add(Constants.SOLR_GROUP_LIMIT_PARAM, Constants.SOLR_GROUP_LIMIT_UNLIMITED);
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

