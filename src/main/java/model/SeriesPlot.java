package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.*;

public class SeriesPlot {
    public static final String SERIES_FIELD_DEFAULT_NAME = "series1";

    private String xAxisField;
    private String yAxisField;
    private String seriesField;
    private boolean usingSeries;
    private boolean xAxisIsDate;
    private boolean yAxisIsDate;
    private boolean seriesIsDate;
    private Double maxX;
    private Double maxY;
    private Double minX;
    private Double minY;
    private long nFound = 0;
    private long nNotNull  = 0;
    private HashMap<String, List<PlotDatum>> seriesData;
    private List<PlotDatum> allData;

    public SeriesPlot(String xAxisField, boolean xAxisIsDate, String yAxisField, boolean yAxisIsDate, String seriesField,
                      boolean seriesIsDate, SolrDocumentList docs) {
        this.xAxisField     = xAxisField;
        this.yAxisField     = yAxisField;
        this.seriesField    = seriesField;
        this.xAxisIsDate    = xAxisIsDate;
        this.yAxisIsDate    = yAxisIsDate;
        this.seriesIsDate   = seriesIsDate;
        this.usingSeries    = !Utils.nullOrEmpty(seriesField);
        this.seriesData     = new HashMap<String, List<PlotDatum>>();
        this.allData        = new ArrayList<PlotDatum>();
        this.init(docs);
    }

    public void init(SolrDocumentList docs) {
        this.nFound = docs.size();

        for(SolrDocument doc : docs) {
            Object xFieldValue = doc.getFieldValue(xAxisField);
            Object yFieldValue = doc.getFieldValue(yAxisField);
            if (xFieldValue == null || yFieldValue == null) continue;

            this.nNotNull++;
            PlotDatum plotDatum = new PlotDatum(xFieldValue, yFieldValue);

            if (this.usingSeries) {
                String series = SolrUtils.getFieldStringValue(doc, seriesField, "");
                if (this.seriesIsDate) {
                    series = DateUtils.getFormattedDateString(series, DateUtils.MONTH_FORMAT);
                }
                List<PlotDatum> pts;
                if (seriesData.containsKey(series)) {
                    pts = updatePlotDataList(plotDatum, seriesData.get(series));
                } else {
                    pts = new ArrayList<PlotDatum>();
                    pts.add(plotDatum);
                }
                seriesData.put(series, pts);
            }

            allData = updatePlotDataList(plotDatum, allData);
            updateMinMaxValues(plotDatum);
        }
    }

    private List<PlotDatum> updatePlotDataList(PlotDatum plotDatum, List<PlotDatum> plotDatumList) {
        int index = plotDatumList.size() - 1;

        if (index >= 0 && plotDatum.isEqual(plotDatumList.get(index))) {
            plotDatumList.get(index).addOccurrence();
        } else {
            plotDatumList.add(plotDatum);
        }
        return plotDatumList;
    }

    private void updateMinMaxValues(PlotDatum datum) {
        double xDouble = datum.getXDoubleValue();
        if (xDouble != Constants.INVALID_DOUBLE) {
            this.maxX = this.maxX == null ? xDouble : Math.max(xDouble, this.maxX);
            this.minX = this.minX == null ? xDouble : Math.min(xDouble, this.minX);
        }

        double yDouble = datum.getYDoubleValue();
        if (yDouble != Constants.INVALID_DOUBLE) {
            this.maxY = this.maxY == null ? yDouble : Math.max(yDouble, this.maxY);
            this.minY = this.minY == null ? yDouble : Math.min(yDouble, this.minY);
        }
    }

    private JSONArray getPlotPtsAsJSON(List<PlotDatum> plotDatumList) {
        JSONArray pts = new JSONArray();
        for(PlotDatum plotDatum : plotDatumList) {
            if (plotDatum.valid()) {
                JSONObject pt = new JSONObject();
                pt.put("x", plotDatum.getFormattedX());
                pt.put("y", plotDatum.getFormattedY());
                pt.put("count", plotDatum.getOccurrences());
                pts.add(pt);
            }
        }
        return pts;
    }

    private Object getRangeValue(Double value, boolean isDate) {
        return isDate && value != null ? DateUtils.DAY_FORMAT.format(new Date(value.longValue())) : value;
    }

    public JSONObject getPlotData() {
        JSONObject series = new JSONObject();
        series.put("xAxis", xAxisField);
        series.put("yAxis", yAxisField);
        series.put("series", seriesField);
        series.put("xAxisIsDate", xAxisIsDate);
        series.put("yAxisIsDate", yAxisIsDate);
        series.put("nFound", this.nFound);
        series.put("nNotNull", this.nNotNull);

        JSONObject range = new JSONObject();
        range.put("min", getRangeValue(minX, xAxisIsDate));
        range.put("max", getRangeValue(maxX, xAxisIsDate));
        series.put("xRange", range);

        range.put("min", getRangeValue(minY, yAxisIsDate));
        range.put("max", getRangeValue(maxY, yAxisIsDate));
        series.put("yRange", range);

        JSONObject data = new JSONObject();
        if (this.usingSeries) {
            JSONObject seriesDataJsonObject = new JSONObject();
            for(Map.Entry<String, List<PlotDatum>> entry : seriesData.entrySet()) {
                seriesDataJsonObject.put(entry.getKey(), getPlotPtsAsJSON(entry.getValue()));
            }
            data.put("series", seriesDataJsonObject);
        }

        JSONObject allDataJsonObject = new JSONObject();
        allDataJsonObject.put(SERIES_FIELD_DEFAULT_NAME, getPlotPtsAsJSON(allData));
        data.put("all", allDataJsonObject);
        series.put("data", data);
        return series;
    }
}
