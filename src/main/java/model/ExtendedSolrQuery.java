package model;

import GatesBigData.constants.solr.Defaults;
import GatesBigData.constants.solr.Operations;
import GatesBigData.constants.solr.QueryParams;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.schema.DateField;

import java.util.*;

public class ExtendedSolrQuery extends SolrQuery {

    public static final int FACET_LIMIT_DEFAULT     = 20;
    public static final int FACET_MINCOUNT_DEFAULT  = 1;

    public ExtendedSolrQuery() {
        super();
        this.setQuery(Defaults.QUERY);
        this.add(QueryParams.WT, Defaults.WT);
    }

    public ExtendedSolrQuery(String queryString) {
        this();
        this.setQuery(queryString);
    }

    public ExtendedSolrQuery(HashMap<String, String> queryParams) {
        this();
        this.setQuery(constructQueryString(queryParams, Operations.BOOLEAN_DEFAULT));
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
        this.add(QueryParams.HIGHLIGHT_FIELDLIST, field);
    }

    public void setHighlightFields(List<String> fields) {
        for(String field : fields) {
            setHighlightField(field);
        }
    }

    public void setHighlightQuery(String queryString, String fq) {
        this.add(QueryParams.HIGHLIGHT_QUERY, queryString + (fq != null ? " " + fq : ""));
    }

    public void setHighlightDefaults() {
        this.setHighlight(true);
        this.setHighlightSimplePre(Defaults.HIGHLIGHT_PRE);
        this.setHighlightSimplePost(Defaults.HIGHLIGHT_POST);
        this.setHighlightSnippets(Defaults.HIGHLIGHT_SNIPPETS);
        this.setHighlightFragsize(Defaults.HIGHLIGHT_FRAGSIZE);
    }

    public void setQueryStart(Integer start) {
        this.setStart(start == null ? Defaults.START : start);
    }

    public void setQueryRows(Integer rows) {
        this.setRows(rows == null ? Defaults.ROWS : rows);
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

    public void setStatsFields(List<String> statsFields) {
        this.setGetFieldStatistics(true);
        for(String s : statsFields) {
            this.setGetFieldStatistics(s);
        }
    }

    public void setFacetDefaults() {
        this.setFacet(true);
        this.setFacetMissing(true);
        this.setFacetLimit(Defaults.FACET_LIMIT);
        this.setFacetMinCount(Defaults.FACET_MINCOUNT);
    }

    public void addFacetFields(List<String> facetFields) {
        for(String facetField : facetFields) {
            this.addFacetField(facetField);
        }
    }

    public void addQueryFacets(FacetFieldEntryList facetFields, SolrCollectionSchemaInfo info) {
        if (Utils.nullOrEmpty(facetFields)) {
            return;
        }

        setFacetDefaults();

        for(FacetFieldEntry field : facetFields) {
            String f = field.getFieldName();
            if (field.fieldTypeIsDate() && !field.isMultiValued()) {
                this.setDateRangeFacet(f, info.getDateRangeStart(f), info.getDateRangeEnd(f), 10);
            } else {
                this.addFacetField(f);
            }
        }
    }

    public void setGroupField(String field) {
        this.add(QueryParams.GROUP_FIELD, field);
    }

    public void setGroupSort(String sort) {
        this.add(QueryParams.GROUP_SORT, sort);
    }

    public void setGroupSortDefault() {
        this.add(QueryParams.GROUP_SORT, Defaults.GROUP_SORT);
    }

    public void setGroupLimit(int limit) {
        this.add(QueryParams.GROUP_LIMIT, "" + limit);
    }

    public void setGroupLimitUnlimited() {
        this.add(QueryParams.GROUP_LIMIT, "" + Defaults.GROUP_LIMIT_UNLIMITED);
    }

    public void setGroup(boolean groupsOn) {
        this.add(QueryParams.GROUP, groupsOn + "");
    }

    public void showGroupNGroups() {
        this.add(QueryParams.GROUP_NGROUPS, Boolean.toString(true));
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
        this.add(QueryParams.FACET_DATE_FIELD, fieldName);
        this.add(QueryParams.fieldFacetDateStartParam(fieldName), DateField.formatExternal(startDate));
        this.add(QueryParams.fieldFacetDateEndParam(fieldName), DateField.formatExternal(endDate));
        this.add(QueryParams.fieldFacetDateGapParam(fieldName), DateUtils.getDateGapString(startDate, endDate, buckets));
    }

    public void addSortClause(String sortField, String order) {
        this.addSortClause(sortField, SolrUtils.getSortOrder(order));
    }

    public void addSortClause(String sortField, ORDER order) {
        this.addSort(new SortClause(sortField, order));
    }

    public void addSortClauses(List<SortClause> clauses) {
        if (Utils.nullOrEmpty(clauses)) {
            this.addSortClause(Defaults.SORT_FIELD, Defaults.SORT_ORDER);
        } else {
            for(SortClause clause : clauses) {
                this.addSort(clause);
            }
        }
    }
}

