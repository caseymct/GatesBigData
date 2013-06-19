package GatesBigData.constants.solr;

import java.util.Arrays;
import java.util.List;

public class FieldTypes extends Solr {
    public static final String DATE                 = "date";
    public static final String STRING               = "string";
    public static final String INT                  = "int";
    public static final String LONG                 = "long";
    public static final String FLOAT                = "float";
    public static final String DOUBLE               = "double";
    public static final String TEXT_GENERAL         = "text_general";
    public static final String PREFIX_TOKEN         = "prefix_token";
    public static final List<String> NUMBER_FIELDS  = Arrays.asList(INT, FLOAT, LONG, DOUBLE);
    public static final List<String> TEXT_FIELDS    = Arrays.asList(STRING, TEXT_GENERAL);

    public static boolean isPrefixTokenType(String t) {
        return t.equals(PREFIX_TOKEN);
    }

    public static boolean isDateType(String t) {
        return t.equals(DATE);
    }
}
