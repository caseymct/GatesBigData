package GatesBigData.constants.solr;

public class QueryParams extends Solr {
    public static final String QUERY                     = "q";
    public static final String FIELDLIST                 = "fl";
    public static final String FILE                      = "file";
    public static final String WT                        = "wt";
    public static final String HIGHLIGHT                 = "hl";
    public static final String HIGHLIGHT_QUERY           = HIGHLIGHT + ".q";
    public static final String HIGHLIGHT_FRAGSIZE        = HIGHLIGHT + ".fragsize";
    public static final String HIGHLIGHT_PRE             = HIGHLIGHT + ".simple.pre";
    public static final String HIGHLIGHT_POST            = HIGHLIGHT + ".simple.post";
    public static final String HIGHLIGHT_SNIPPETS        = HIGHLIGHT + ".snippets";
    public static final String HIGHLIGHT_FIELDLIST       = HIGHLIGHT + ".fl";
    public static final String FACET_DATE_FIELD          = "facet.date";
    public static final String FACET_DATE_START          = FACET_DATE_FIELD + ".start";
    public static final String FACET_DATE_END            = FACET_DATE_FIELD + ".end";
    public static final String FACET_DATE_GAP            = FACET_DATE_FIELD + ".gap";
    public static final String GROUP                     = "group";
    public static final String GROUP_LIMIT               = GROUP + ".limit";
    public static final String GROUP_FIELD               = GROUP + ".field";
    public static final String GROUP_SORT                = GROUP + ".sort";
    public static final String GROUP_NGROUPS             = GROUP + ".ngroups";

    public static String fieldFacetDateStartParam(String fieldName) {
        return "f." + fieldName + "." + FACET_DATE_START;
    }

    public static String fieldFacetDateEndParam(String fieldName) {
        return "f." + fieldName + "." + FACET_DATE_END;
    }

    public static String fieldFacetDateGapParam(String fieldName) {
        return "f." + fieldName + "." + FACET_DATE_GAP;
    }
}
