package model.analysis;

import GatesBigData.constants.Constants;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.Utils;

import java.util.Date;

public class PlotDatum {
    private Object x;
    private Object y;
    private double xDoubleValue = 0.0;
    private double yDoubleValue = 0.0;
    private int occurrences = 1;

    public PlotDatum(Object x, Object y) {
        this.x = x;
        this.y = y;
        this.xDoubleValue = getDoubleValue(this.x);
        this.yDoubleValue = getDoubleValue(this.y);
    }

    public double getDoubleValue(Object o) {
        if (!Utils.nullOrEmpty(o)) {
            if (o instanceof Date) {
                return ((Date) o).getTime();
            } else if (o instanceof Float) {
                return ((Float) o).doubleValue();
            } else if (o instanceof Integer) {
                return ((Integer) o).doubleValue();
            } else if (o instanceof Long) {
                return ((Long) o).doubleValue();
            } else if (o instanceof String) {
                return Double.parseDouble((String) o);
            }
        }
        return Constants.INVALID_DOUBLE;
    }

    public void addOccurrence() {
        this.occurrences++;
    }

    public boolean valid() {
        return !Utils.nullOrEmpty(this.x) && !Utils.nullOrEmpty(this.y);
    }

    public Object getX() {
        return this.x;
    }

    public Object getY() {
        return this.y;
    }

    private Object getFormattedDate(Date d) {
        return DateUtils.getFormattedDateString(d, DateUtils.DAY_DATE_FORMAT);
    }

    public Object getFormattedX() {
        return this.x instanceof Date ? getFormattedDate((Date) this.x) : this.x;
    }

    public Object getFormattedY() {
        return this.y instanceof Date ? getFormattedDate((Date) this.y) : this.y;
    }

    public double getXDoubleValue() {
        return this.xDoubleValue;
    }

    public double getYDoubleValue() {
        return this.yDoubleValue;
    }

    public boolean isEqual(PlotDatum newPlotDatum) {
        return this.x.equals(newPlotDatum.getX()) && this.y.equals(newPlotDatum.getY());
    }

    public int getOccurrences() {
        return this.occurrences;
    }
}
