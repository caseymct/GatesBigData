package model;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<String> {
    Map<String, Number> base;
    public ValueComparator(Map<String, Number> base) {
        this.base = base;
    }

    public int compare(String a, String b) {
        Number n1 = base.get(a);
        Number n2 = base.get(b);

        double d1, d2;

        if (n1 instanceof Double) {
            d1 = n1.doubleValue();
            d2 = n2.doubleValue();
        } else if (n1 instanceof Long) {
            d1 = (double) n1.longValue();
            d2 = (double) n2.longValue();
        } else if (n1 instanceof Integer) {
            d1 = (double) n1.intValue();
            d2 = (double) n2.intValue();
        } else if (n1 instanceof Float) {
            d1 = (double) n1.floatValue();
            d2 = (double) n2.floatValue();
        } else if (n1 instanceof Short) {
            d1 = (double) n1.shortValue();
            d2 = (double) n2.shortValue();
        } else {
            d1 = (double) n1.byteValue();
            d2 = (double) n2.byteValue();
        }
        return (d1 == d2) ? a.compareTo(b) : (d1 > d2 ? -1 : 1);
    }
}
