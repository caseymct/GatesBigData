package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.schema.DateField;

import java.util.*;

public class ExtendedSolrQuery extends SolrQuery {

    public static final int GROUP_UNLIMITED         = -1;
    public static final int FACET_LIMIT_DEFAULT     = 20;
    public static final int FACET_MINCOUNT_DEFAULT  = 1;

    public ExtendedSolrQuery() {
        super();
        this.add(Constants.SOLR_PARAM_WT, Constants.SOLR_DEFAULT_VALUE_WT);
    }

    public ExtendedSolrQuery(String queryString) {
        this();
        this.setQuery(queryString);
    }

    public ExtendedSolrQuery(HashMap<String, String> queryParams) {
        this();
        this.setQuery(constructQueryString(queryParams, Constants.SOLR_BOOLEAN_DEFAULT));
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

    public void setHighlightField(String field) {
        this.add(Constants.SOLR_PARAM_HIGHLIGHT_FIELDLIST, field);
    }

    public void setHighlightFields(List<String> fields) {
        for(String field : fields) {
            setHighlightField(field);
        }
    }

    public void setHighlightQuery(String queryString, String fq) {
        this.add(Constants.SOLR_PARAM_HIGHLIGHT_QUERY, queryString + (fq != null ? " " + fq : ""));
    }

    public void setHighlightDefaults() {
        this.setHighlight(true);
        this.setHighlightSimplePre(Constants.SOLR_DEFAULT_VALUE_HIGHLIGHT_PRE);
        this.setHighlightSimplePost(Constants.SOLR_DEFAULT_VALUE_HIGHLIGHT_POST);
        this.setHighlightSnippets(Constants.SOLR_DEFAULT_VALUE_HIGHLIGHT_SNIPPETS);
        this.setHighlightFragsize(Constants.SOLR_DEFAULT_VALUE_HIGHLIGHT_FRAGSIZE);
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

    public void setFacetDefaults() {
        this.setFacet(true);
        this.setFacetMissing(true);
        this.setFacetLimit(FACET_LIMIT_DEFAULT);
        this.setFacetMinCount(FACET_MINCOUNT_DEFAULT);
    }

    public void setGroupField(String field) {
        this.add(Constants.SOLR_PARAM_GROUP_FIELD, field);
    }

    public void setGroupSort(String sort) {
        this.add(Constants.SOLR_PARAM_GROUP_SORT, sort);
    }

    public void setGroupSortDefault() {
        this.add(Constants.SOLR_PARAM_GROUP_SORT, Constants.SOLR_DEFAULT_VALUE_GROUP_SORT);
    }

    public void setGroupLimit(int limit) {
        this.add(Constants.SOLR_PARAM_GROUP_LIMIT, "" + limit);
    }

    public void setGroupLimitUnlimited() {
        this.add(Constants.SOLR_PARAM_GROUP_LIMIT, "" + Constants.SOLR_GROUP_LIMIT_UNLIMITED);
    }

    public void setGroup(boolean groupsOn) {
        this.add(Constants.SOLR_PARAM_GROUP, groupsOn + "");
    }

    public void setGroupingDefaults(String groupField) {
        setGroup(true);
        setGroupSortDefault();
        setGroupField(groupField);
    }

    public void setDateRangeFacet(String fieldName, Date startDate, Date endDate, int buckets) {
        if (startDate == null || endDate == null) {
            return;
        }
        this.add(Constants.SOLR_PARAM_FACET_DATE_FIELD, fieldName);
        this.add(Constants.SOLR_FACET_DATE_START(fieldName), DateField.formatExternal(startDate));
        this.add(Constants.SOLR_FACET_DATE_END(fieldName), DateField.formatExternal(endDate));
        this.add(Constants.SOLR_FACET_DATE_GAP(fieldName), DateUtils.getDateGapString(startDate, endDate, buckets));
    }
}

