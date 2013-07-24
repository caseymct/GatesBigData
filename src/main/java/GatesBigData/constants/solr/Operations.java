package GatesBigData.constants.solr;

import java.util.HashMap;

public class Operations extends Solr {
    public static final String BOOLEAN_INTERSECTION = "OR";
    public static final String BOOLEAN_UNION        = "AND";
    public static final String BOOLEAN_DEFAULT      = BOOLEAN_INTERSECTION;

    public static final int OPERATION_ADD           = 1;
    public static final int OPERATION_UPDATE        = 2;
    public static final int OPERATION_DELETE        = 3;
    public static final int OPERATION_OPTIMIZE      = 4;

    public static final HashMap<Integer, String> OPERATION_MSGS = new HashMap<Integer, String>() {{
        put(OPERATION_ADD,           "Add to index");
        put(OPERATION_DELETE,        "Delete index");
        put(OPERATION_UPDATE,        "Update index");
        put(OPERATION_OPTIMIZE,      "Optimize index");
    }};
}
