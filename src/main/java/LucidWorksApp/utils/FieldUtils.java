package LucidWorksApp.utils;

/**
 * Created by IntelliJ IDEA.
 * User: caseymctaggart
 * Date: 7/26/12
 * Time: 3:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class FieldUtils extends Utils {

    public static String createFieldJsonBlock(String fieldName, String defaultValue) {
        return  "{ \"name\": \"" + fieldName + "\"," +
                   "\"default_value\": \"" + defaultValue + "\"," +
                   "\"multi_valued\" : true," +
                   "\"stored\": true," +
                   "\"indexed\": true," +
                   "\"facet\": true, " +
                   "\"index_for_spellcheck\": true, " +
                   "\"synonym_expansion\": true," +
                   "\"field_type\": \"text_en\", " +
                   "\"copy_fields\": [ \"text_medium\", \"text_all\" ] }";
    }

    public static String createField(String collectionName, String fieldName, String fieldValue) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + "/fields";

        if (!fieldExists(fieldName, url + "/" + fieldName)) {
            return HttpClientUtils.httpPostRequest(url, FieldUtils.createFieldJsonBlock(fieldName, fieldValue));
        }
        return "";
    }

    public static boolean fieldExists(String fieldName, String url) {
        String response = HttpClientUtils.httpGetRequest(url);

        return !(response.contains("\"http_status_code\":404"));

    }
}
