package GatesBigData.constants;

public class XmlConfig {
    public static final String GENERAL_CONF_FILE     = "conf/reindexer-config.xml";
    public static final String COLLECTION_CONF_FILE  = "collection-config.xml";

    public static final String COLLECTION_TAG               = "collection";
    public static final String NAME_TAG                     = "name";
    public static final String VALUE_TAG                    = "value";
    public static final String PROPERTY_TAG                 = "property";

    public static final String PROPERTY_COLLECTION_NAME  = "collection.name";
    public static final String PROPERTY_FIELDS_PREFIX    = "fields";
    public static final String PROPERTY_FIELDS_AUDIT     = "fields.audit";
    public static final String PROPERTY_FIELDS_FACET     = "fields.facet";
    public static final String PROPERTY_FIELDS_PREVIEW   = "fields.preview";
    public static final String PROPERTY_FIELDS_TABLE     = "fields.table";
    public static final String PROPERTY_FIELDS_WORDTREE  = "fields.wordtree";
    public static final String PROPERTY_FIELDS_XAXIS     = "fields.xaxis";
    public static final String PROPERTY_FIELDS_YAXIS     = "fields.yaxis";
    public static final String PROPERTY_FIELDS_SERIES    = "fields.series";

    public static final String PROPERTY_REPORT_PREFIX                       = "report";
    public static final String PROPERTY_REPORT_AGGREGATE_PREFIX             = PROPERTY_REPORT_PREFIX + ".aggregate";
    public static final String PROPERTY_REPORT_FIELDS_SUFFIX                = "fields";
    public static final String PROPERTY_REPORT_TITLE                        = "report.title";
    public static final String PROPERTY_REPORT_FILTERS_FIELDS               = "report.filters.fields";
    public static final String PROPERTY_REPORT_FILTERS_DATEFIELDS           = "report.filters.datefields";
    public static final String PROPERTY_REPORT_FILTERS_SORTFIELDS           = "report.filters.sortfields";
    public static final String PROPERTY_REPORT_DISPLAY_FIELDS               = "report.display.fields";
    public static final String PROPERTY_REPORT_EXPORT_FIELDS                = "report.export.fields";
    public static final String PROPERTY_REPORT_AGGREGATE_FIELD              = "report.aggregate.field";
    public static final String PROPERTY_REPORT_AGGREGATE_FIELD_DISPLAY      = "report.aggregate.field.display";
    public static final String PROPERTY_REPORT_AGGREGATE_FIELD_SOLRFIELD    = "report.aggregate.field.solrfield";

    public static boolean compareStringsIgnoreCaseAndWhitespace(String s1, String s2) {
        return s1 != null && s2 != null && s1.trim().toUpperCase().equals(s2.trim().toUpperCase());
    }

    public static boolean isTag(String tag, String toCompare) {
        return compareStringsIgnoreCaseAndWhitespace(toCompare, tag);
    }


    public static boolean isPropertyTag(String tag) {
        return compareStringsIgnoreCaseAndWhitespace(PROPERTY_TAG, tag);
    }

    public static boolean isNameTag(String tag) {
        return compareStringsIgnoreCaseAndWhitespace(NAME_TAG, tag);
    }

    public static boolean isValueTag(String tag) {
        return compareStringsIgnoreCaseAndWhitespace(VALUE_TAG, tag);
    }

    public static boolean isReportTag(String tag) {
        return tag.toLowerCase().startsWith(PROPERTY_REPORT_PREFIX);
    }

    public static boolean isReportFieldsTag(String tag) {
        return tag.toLowerCase().endsWith(PROPERTY_REPORT_FIELDS_SUFFIX);
    }

    public static boolean isReportAggregateFieldTag(String name) {
        return compareStringsIgnoreCaseAndWhitespace(name, PROPERTY_REPORT_AGGREGATE_FIELD);
    }

    public static boolean isFieldsTag(String tag) {
        return tag.toLowerCase().startsWith(PROPERTY_FIELDS_PREFIX);
    }
}
