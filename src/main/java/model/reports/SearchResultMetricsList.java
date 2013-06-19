package model.reports;

import GatesBigData.utils.JSONUtils;
import GatesBigData.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.util.*;

//aggregate:[{name:"TOTAL_COST", docfield:"TOTAL_COST"},{name:"TOTAL_QUANTITY",docfield:"PRIMARY_QUANTITY"}]
public class SearchResultMetricsList {
    HashMap<String, SearchResultMetrics> searchResultMetricsMap = new HashMap<String, SearchResultMetrics>();

    Set<String> viewFields = new HashSet<String>();

    public SearchResultMetricsList(JSONArray definitionsData) {
        for(int i = 0; i < definitionsData.size(); i++) {
            SearchResultMetrics s = new SearchResultMetrics(definitionsData.getJSONObject(i));
            searchResultMetricsMap.put(s.getField(), s);
            viewFields.add(s.getField());
        }
    }

    public List<String> getViewFields() {
        return new ArrayList<String>(this.viewFields);
    }

    public String getViewFieldsString() {
        return StringUtils.join(viewFields, ",");
    }

    public void writeMetrics(Map<String, FieldStatsInfo> fieldStatsInfoMap, JsonGenerator g) throws IOException {

        for(Map.Entry<String, FieldStatsInfo> entry : fieldStatsInfoMap.entrySet()) {    // e.g. "TOTAL_QUANTITY"
            String displayFieldName = entry.getKey();
            FieldStatsInfo info     = entry.getValue();

            g.writeArrayFieldStart(displayFieldName);
            String docField = searchResultMetricsMap.get(displayFieldName).getName();

            g.writeStartObject();
            Utils.writeValueByType("name", docField, g);
            Utils.writeValueByType("min", info.getMin(), g);
            Utils.writeValueByType("max", info.getMax(), g);
            Utils.writeValueByType("count", info.getCount(), g);
            Utils.writeValueByType("missing", info.getMissing(), g);
            Utils.writeValueByType("sum", info.getSum(), g);
            Utils.writeValueByType("mean", info.getMean(), g);
            Utils.writeValueByType("stddev", info.getStddev(), g);
            g.writeEndObject();

            g.writeEndArray();
        }
    }

    class SearchResultMetrics {
        String name;
        String field;

        SearchResultMetrics(JSONObject o) {
            this.name  = JSONUtils.getStringValue(o, ReportConstants.AggregateMetrics.NAME_KEY);
            this.field = JSONUtils.getStringValue(o, ReportConstants.AggregateMetrics.DOCFIELD_KEY);
        }

        public String getField() {
            return this.field;
        }

        public String getName() {
            return this.name;
        }
    }
}
