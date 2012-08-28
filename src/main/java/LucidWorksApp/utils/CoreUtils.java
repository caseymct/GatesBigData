package LucidWorksApp.utils;

import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.List;


public class CoreUtils extends Utils {


    public static Object getCollectionProperty(String collectionName, String propertyName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + INFO_ENDPOINT;
        String collectionDetails = HttpClientUtils.httpGetRequest(url);

        return JsonParsingUtils.getPropertyFromJsonString(propertyName, collectionDetails);
    }

    public static String getAllCollectionProperties(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + INFO_ENDPOINT;
        return HttpClientUtils.httpGetRequest(url);
    }

    public static String deleteIndexForCollection(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + INDEX_ENDPOINT;
        String urlParams = "?key=iaccepttherisk";

        FieldUtils.clearCSVFilesUploadedField(collectionName);

        return HttpClientUtils.httpDeleteRequest(url + urlParams);
    }

    public static String createCollection(HashMap<String, Object> properties) {
        return HttpClientUtils.httpJsonPostRequest(SERVER + COLLECTIONS_ENDPOINT,
                JsonParsingUtils.constructJsonStringFromProperties(properties));
    }
}
