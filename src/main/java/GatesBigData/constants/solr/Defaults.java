package GatesBigData.constants.solr;

import org.apache.solr.client.solrj.SolrQuery;

import static GatesBigData.constants.solr.FieldNames.SCORE;

public class Defaults {
    public static final int START                    = 0;
    public static final int ROWS                     = 10;
    public static final int HIGHLIGHT_SNIPPETS       = 5;
    public static final int HIGHLIGHT_FRAGSIZE       = 50;
    public static final int MAX_FACET_RESULTS        = 20;
    public static final int GROUP_LIMIT_UNLIMITED    = -1;
    public static final int FACET_LIMIT              = 20;
    public static final int FACET_MINCOUNT           = 1;

    public static final String QUERY_DEFAULT             = "*:*";
    public static final String WT                        = "json";
    public static final String SORT_FIELD_DEFAULT        = "score";
    public static final String HIGHLIGHT_PRE_DEFAULT     = "<span class='highlight_text'>";
    public static final String HIGHLIGHT_POST_DEFAULT    = "</span>";
    public static final String GROUP_SORT                = SCORE + " " + SolrQuery.ORDER.desc;
    public static final SolrQuery.ORDER SORT_ORDER       = SolrQuery.ORDER.asc;
    public static final SolrQuery.SortClause SORT_CLAUSE = new SolrQuery.SortClause(SORT_FIELD_DEFAULT, SORT_ORDER);

    // Group limit for suggestions. This is how many results are returned per group
    public static final int SUGGESTION_GROUP_LIMIT                 = 1;
    public static final int ANALYSIS_GROUP_LIMIT                   = 0;  // return 0 results per group (for speed)
    public static final int ANALYSIS_ROWS_LIMIT                    = 100; // return all groups... -1 no longer works?
}
