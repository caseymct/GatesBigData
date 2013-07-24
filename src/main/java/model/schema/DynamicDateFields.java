package model.schema;

import model.search.ExtendedSolrQuery;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.*;

import static GatesBigData.utils.SolrUtils.getFieldDateValue;
import static GatesBigData.utils.Utils.nullOrEmpty;

public class DynamicDateFields {
    public static final String DATE_SUFFIX              = "_DATE";
    public static final String DATE_RANGE_START_SUFFIX  = "_RANGE_START";
    public static final String DATE_RANGE_END_SUFFIX    = "_RANGE_END";

    List<String> dateFields  = new ArrayList<String>();
    String dateQuery = "";
    String dateFl    = "";
    // dateField to date range start and end field names map
    HashMap<String, List<String>> map = new HashMap<String, List<String>>();
    HashMap<String, Date> dateMap     = new HashMap<String, Date>();

    DynamicDateFields(List<String> dateFields) {
        this.dateFields = dateFields;

        List<String> queries = new ArrayList<String>();
        Set<String> rangeFields = new HashSet<String>();
        for(String dateField : dateFields) {
            queries.add(getDateRangeStartField(dateField) + ":*");
            List<String> dateRangeFieldNames = getDateRangeFieldNames(dateField);
            rangeFields.addAll(dateRangeFieldNames);
            this.map.put(dateField, dateRangeFieldNames);
        }
        this.dateQuery = StringUtils.join(queries, " ");
        this.dateFl    = StringUtils.join(rangeFields, ",");
    }

    public boolean hasDateRanges() {
        return nullOrEmpty(dateMap);
    }

    public boolean hasDateRangeForField(String field) {
        List<String> dateFields = map.get(field);
        return !nullOrEmpty(dateMap) && dateMap.containsKey(dateFields.get(0)) && dateMap.containsKey(dateFields.get(1));
    }

    public void update(SolrDocumentList docs) {
        if (docs.size() == 0) {
            return;
        }

        for(SolrDocument doc : docs) {
            for(String field : doc.getFieldNames()) {
                this.dateMap.put(field, getFieldDateValue(doc, field));
            }
        }
    }

    public ExtendedSolrQuery getDateSolrQuery() {
        ExtendedSolrQuery query = new ExtendedSolrQuery(this.dateQuery);
        query.setFields(this.dateFl);
        return query;
    }

    public Date getStart(String s) {
        return getDate(getDateRangeStartField(s));
    }

    public Date getEnd(String s) {
        return getDate(getDateRangeEndField(s));
    }

    public Date getDate(String dateField) {
        return dateMap.containsKey(dateField) ? dateMap.get(dateField) : null;
    }

    public String getDateField(String s) {
        if (s.endsWith(DATE_RANGE_START_SUFFIX) || s.endsWith(DATE_RANGE_END_SUFFIX)) {
            return s.replace(DATE_RANGE_START_SUFFIX, "").replace(DATE_RANGE_END_SUFFIX, "");
        }
        return s.endsWith(DATE_SUFFIX) ? s : s + DATE_SUFFIX;
    }

    public String getDateRangeStartField(String s) {
        return getDateField(s) + DATE_RANGE_START_SUFFIX;
    }

    public String getDateRangeEndField(String s) {
        return getDateField(s) + DATE_RANGE_END_SUFFIX;
    }

    public List<String> getDateRangeFieldNames(String s) {
        return Arrays.asList(getDateRangeStartField(s), getDateRangeEndField(s));
    }

    public static boolean isDateRangeField(String s) {
        return s.endsWith(DATE_RANGE_END_SUFFIX) || s.endsWith(DATE_RANGE_START_SUFFIX);
    }
}
