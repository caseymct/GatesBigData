package LucidWorksApp.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: caseymctaggart
 * Date: 7/26/12
 * Time: 2:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utils {
    public static final String COLLECTIONS_ENDPOINT = "/api/collections";
    public static final String SERVER = "http://localhost:8888";
    public static final String COLLECTIONS_TEMPLATES_ENDPOINT = "/api/collectiontemplates";
    public static final String DATASOURCES_ENDPOINT = "/datasources";
    public static final String INFO_ENDPOINT = "/info";
    public static final String INDEX_ENDPOINT = "/index";

    public static List<String> convertObjectListToStringList(List<Object> objectList) {
        List<String> stringList = new ArrayList<String>();
        for (Object o : objectList) {
            if (o instanceof String) {
                stringList.add((String) o);
            }
        }
        return stringList;
    }
}
