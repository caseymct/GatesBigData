package LucidWorksApp.utils;

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
            return HttpClientUtils.httpJsonPostRequest(url, FieldUtils.createFieldJsonBlock(fieldName, fieldValue));
        }
        return "";
    }

    public static boolean fieldExists(String fieldName, String url) {
        String response = HttpClientUtils.httpGetRequest(url);

        return !(response.contains("\"http_status_code\":404"));

    }
}
