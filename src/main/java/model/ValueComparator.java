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

        if (n1 instanceof Double) {
            return (n1.doubleValue() >= n2.doubleValue()) ? -1 : 1;
        } else if (n1 instanceof Long) {
            return (n1.longValue() >= n2.longValue()) ? -1 : 1;
        } else if (n1 instanceof Integer) {
            return (n1.intValue() >= n2.intValue()) ? -1 : 1;
        } else if (n1 instanceof Float) {
            return (n1.floatValue() >= n2.floatValue()) ? -1 : 1;
        }
        return (n1.byteValue() >= n2.byteValue()) ? -1 : 1;
    }
}
