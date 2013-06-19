package model;

import GatesBigData.constants.Constants;
import GatesBigData.utils.*;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.*;

public class SeriesPlot {
    public static final String SERIES_FIELD_DEFAULT_NAME    = "series1";

    public static final String RESPONSE_KEY_X_AXIS          = "xAxis";
    public static final String RESPONSE_KEY_Y_AXIS          = "yAxis";
    public static final String RESPONSE_KEY_SERIES          = "series";
    public static final String RESPONSE_KEY_X_AXIS_DATE     = "xAxisIsDate";
    public static final String RESPONSE_KEY_Y_AXIS_DATE     = "yAxisIsDate";
    public static final String RESPONSE_KEY_NUM_FOUND       = "nFound";
    public static final String RESPONSE_KEY_NUM_NOT_NULL    = "nNotNull";
    public static final String RESPONSE_KEY_MIN_VALUE       = "min";
    public static final String RESPONSE_KEY_MAX_VALUE       = "max";
    public static final String RESPONSE_KEY_X_RANGE         = "xRange";
    public static final String RESPONSE_KEY_Y_RANGE         = "yRange";
    public static final String RESPONSE_KEY_COUNT           = "count";
    public static final String RESPONSE_KEY_X               = "x";
    public static final String RESPONSE_KEY_Y               = "y";
    public static final String RESPONSE_KEY_UNIQUE_X_DATES  = "uniqueXDates";
    public static final String RESPONSE_KEY_UNIQUE_Y_DATES  = "uniqueYDates";
    public static final String RESPONSE_KEY_ALL_DATA        = "all";
    public static final String RESPONSE_KEY_DATA            = "data";

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
    private long nFound     = 0;
    private long nNotNull   = 0;
    private HashMap<String, List<PlotDatum>> seriesData;
    private List<PlotDatum> allData;
    private SortedSet<Long> uniqueXDateData;
    private SortedSet<Long> uniqueYDateData;
    private Comparator<Long> dateComparator = new Comparator<Long>(){
        public int compare(Long d1, Long d2) {
            //long t1 = d1.getTime(), t2 = d2.getTime();
            long t1 = d1, t2 = d2;
            if (t1 == t2) return 0;
            return (t1 < t2) ? -1 : 1;
        }
    };

    public SeriesPlot(String xAxisField, boolean xAxisIsDate, String yAxisField, boolean yAxisIsDate, String seriesField,
                      boolean seriesIsDate, SolrDocumentList docs) {
        this.xAxisField         = xAxisField;
        this.yAxisField         = yAxisField;
        this.seriesField        = seriesField;
        this.xAxisIsDate        = xAxisIsDate;
        this.yAxisIsDate        = yAxisIsDate;
        this.seriesIsDate       = seriesIsDate;
        this.usingSeries        = !Utils.nullOrEmpty(seriesField);
        this.seriesData         = new HashMap<String, List<PlotDatum>>();
        this.allData            = new ArrayList<PlotDatum>();
        this.uniqueXDateData    = new TreeSet<Long>();
        this.uniqueYDateData    = new TreeSet<Long>();
        this.nFound             = docs.size();

        this.init(docs);
    }

    public void init(SolrDocumentList docs) {
        for(SolrDocument doc : docs) {
            Object xFieldValue = doc.getFieldValue(xAxisField);
            Object yFieldValue = doc.getFieldValue(yAxisField);
            if (xFieldValue == null || yFieldValue == null) continue;

            this.nNotNull++;
            PlotDatum plotDatum = new PlotDatum(xFieldValue, yFieldValue);

            if (this.usingSeries) {
                String series = SolrUtils.getFieldStringValue(doc, seriesField, "");
                if (this.seriesIsDate) {
                    series = DateUtils.getFormattedDateString(series, DateUtils.MONTH_DATE_FORMAT);
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
            updateUniqueDateSets(xFieldValue, yFieldValue);
            updateMinMaxValues(plotDatum);
        }
    }

    private void updateUniqueDateSets(Object x, Object y) {
        if (this.xAxisIsDate && x instanceof Date) {
            uniqueXDateData.add(((Date) x).getTime());
        }
        if (this.yAxisIsDate && y instanceof Date) {
            uniqueYDateData.add(((Date) y).getTime());
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
                pt.put(RESPONSE_KEY_X,      plotDatum.getFormattedX());
                pt.put(RESPONSE_KEY_Y,      plotDatum.getFormattedY());
                pt.put(RESPONSE_KEY_COUNT,  plotDatum.getOccurrences());
                pts.add(pt);
            }
        }
        return pts;
    }

    private Object getRangeValue(Double value, boolean isDate) {
        if (!isDate || value == null) {
            return value;
        }
        //return isDate && value != null ? DateUtils.DAY_FORMAT.format(new Date(value.longValue())) : value;
        return DateUtils.getFormattedDateString(new Date(value.longValue()), DateUtils.DAY_DATE_FORMAT);
    }

    public JSONObject getPlotData() {
        JSONObject series = new JSONObject();
        series.put(RESPONSE_KEY_X_AXIS, this.xAxisField);
        series.put(RESPONSE_KEY_Y_AXIS, this.yAxisField);
        series.put(RESPONSE_KEY_SERIES, this.seriesField);
        series.put(RESPONSE_KEY_X_AXIS_DATE, this.xAxisIsDate);
        series.put(RESPONSE_KEY_Y_AXIS_DATE, this.yAxisIsDate);
        series.put(RESPONSE_KEY_NUM_FOUND, this.nFound);
        series.put(RESPONSE_KEY_NUM_NOT_NULL, this.nNotNull);
        series.put(RESPONSE_KEY_X_RANGE, getRangeObject(this.minX, this.maxX, this.xAxisIsDate));
        series.put(RESPONSE_KEY_Y_RANGE, getRangeObject(this.minY, this.maxY, this.yAxisIsDate));
        series.put(RESPONSE_KEY_DATA, getDataObject());

        return series;
    }

    private JSONObject getDataObject() {
        JSONObject data = new JSONObject();

        if (this.usingSeries) {
            data.put(RESPONSE_KEY_SERIES, getSeriesDataObject());
        }

        if (this.xAxisIsDate) {
            data.put(RESPONSE_KEY_UNIQUE_X_DATES, JSONUtils.convertCollectionToJSONArray(this.uniqueXDateData));
        }

        if (this.yAxisIsDate) {
            data.put(RESPONSE_KEY_UNIQUE_Y_DATES, JSONUtils.convertCollectionToJSONArray(this.uniqueYDateData));
        }

        JSONObject allDataJsonObject = new JSONObject();
        allDataJsonObject.put(SERIES_FIELD_DEFAULT_NAME, getPlotPtsAsJSON(allData));
        data.put(RESPONSE_KEY_ALL_DATA, allDataJsonObject);
        return data;
    }

    private JSONObject getSeriesDataObject() {
        JSONObject seriesDataJsonObject = new JSONObject();
        for(Map.Entry<String, List<PlotDatum>> entry : seriesData.entrySet()) {
            seriesDataJsonObject.put(entry.getKey(), getPlotPtsAsJSON(entry.getValue()));
        }
        return seriesDataJsonObject;
    }

    private JSONObject getRangeObject(double min, double max, boolean axisIsDate) {
        JSONObject range = new JSONObject();
        range.put(RESPONSE_KEY_MIN_VALUE, getRangeValue(min, axisIsDate));
        range.put(RESPONSE_KEY_MAX_VALUE, getRangeValue(max, axisIsDate));
        return range;
    }
}
