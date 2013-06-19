package model.reports;

import GatesBigData.utils.JSONUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.List;

public class ReportConstants {

    public static final String REPORT_TITLE_KEY             = "title";
    public static final String REPORT_DISPLAY_KEY           = "display";
    public static final String NAMES_TO_DISPLAY_NAMES_KEY   = "namestodisplaynames";
    public static final String FIELDS_TO_EXPORT_KEY         = "exportfields";

    public static List<String> getAsList(JSONObject o, String key) {
        return JSONUtils.convertJSONArrayToStringList(JSONUtils.getJSONArrayValue(o, key, new JSONArray()));
    }

    public static List<String> getFieldsToDisplay(JSONObject o) {
        return getAsList(o, REPORT_DISPLAY_KEY);
    }

    public static class Filters {
        public static final String FILTERS_KEY                      = "filters";
        public static final String SORT_FILTERS_KEY                 = "sortfilters";

        public static final String DATE_FILTERS_KEY                 = "datefilters";
        public static final String DATE_FILTERS_NAME_KEY            = "name";
        public static final String DATE_FILTERS_RANGE_START_KEY     = "start";
        public static final String DATE_FILTERS_RANGE_END_KEY       = "end";

        public static List<String> getFilters(JSONObject o) {
            return getAsList(o, FILTERS_KEY);
        }

        public static List<String> getDateFilters(JSONObject o) {
            return getAsList(o, DATE_FILTERS_KEY);
        }

        public static List<String> getSortFilters(JSONObject o) {
            return getAsList(o, SORT_FILTERS_KEY);
        }
    }

    public static class DisplayNames {
        public static final String NAME_KEY         = "name";
        public static final String DISPLAY_NAME_KEY = "displayname";
    }

    public static class AggregateMetrics {
        public static final String AGGREGATE_KEY    = "aggregate";
        public static final String NAME_KEY         = "name";
        public static final String DOCFIELD_KEY     = "docfield";
    }
}
