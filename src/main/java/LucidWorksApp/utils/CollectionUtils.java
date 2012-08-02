package LucidWorksApp.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CollectionUtils extends Utils {

    private static List<String> collectionDetailProperties = Arrays.asList(
            "free_disk_space", "index_last_modified", "index_has_deletions", "data_dir", "index_size",
            "index_directory", "collection_name", "index_is_optimized", "index_size_bytes", "free_disk_bytes",
            "index_max_doc", "index_num_docs", "index_version", "index_is_current", "root_dir", "instance_dir",
            "total_disk_space", "total_disk_bytes");

    public static List<String> getCollectionNames() {
        String json = HttpClientUtils.httpGetRequest(SERVER + COLLECTIONS_ENDPOINT);

        return convertObjectListToStringList(JsonParsingUtils.getPropertiesFromDataSourceJson("name", json));
    }

    public static String getCollectionInstanceDir(String collectionName) {
        String collectionDetails = HttpClientUtils.httpGetRequest(SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName);
        return (String) JSONObject.fromObject(collectionDetails).get("instance_dir");
    }

    public static Object getCollectionProperty(String collectionName, String propertyName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + INFO_ENDPOINT;
        String collectionDetails = HttpClientUtils.httpGetRequest(url);

        if (collectionDetailProperties.contains(propertyName)) {
            return JsonParsingUtils.getPropertyFromJsonString(propertyName, collectionDetails);
        }

        return null;
    }

    public static String getAllCollectionProperties(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + INFO_ENDPOINT;
        return HttpClientUtils.httpGetRequest(url);
    }

    public static String deleteIndexForCollection(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + INDEX_ENDPOINT;
        return HttpClientUtils.httpDeleteRequest(url);
    }
}
