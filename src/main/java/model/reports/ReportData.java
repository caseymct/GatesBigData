package model.reports;

import model.FacetFieldEntryList;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
@Scope(value = "session", proxyMode = ScopedProxyMode.TARGET_CLASS)
public class ReportData {
    private HashMap<String, CollectionReportData> reportDataMap;

    public ReportData() {
        this.reportDataMap = new HashMap<String, CollectionReportData>();
    }

    public void add(String collection, JSONObject data, JSONArray displayNames, List<String> viewFields, FacetFieldEntryList allFacets) {
        if (!hasCollectionData(collection)) {
            reportDataMap.put(collection, new CollectionReportData(data, displayNames, viewFields, allFacets));
        }
    }

    public CollectionReportData getCollectionReportData(String collection) {
        return hasCollectionData(collection) ? reportDataMap.get(collection) : null;
    }

    public boolean hasCollectionData(String collection) {
        return reportDataMap.containsKey(collection);
    }
}
