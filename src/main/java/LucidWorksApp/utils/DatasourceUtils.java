package LucidWorksApp.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: caseymctaggart
 * Date: 7/27/12
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class DatasourceUtils extends Utils {


    public static int getNumberOfDataSources(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;
        String response = HttpClientUtils.httpGetRequest(url);

        return JSONArray.fromObject(response).size();
    }

    public static String getPropertiesForAllDataSources(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        return HttpClientUtils.httpGetRequest(url);
    }

    public static String getAllDataSourceProperties(String collectionName, int datasourceId) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT + "/" + datasourceId;
        return HttpClientUtils.httpGetRequest(url);
    }

    public static HashMap<String, Object> getDataSourceProperties(String collectionName, int datasourceId, List<String> properties) {

        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT + "/" + datasourceId;
        String allProperties = HttpClientUtils.httpGetRequest(url);
        HashMap<String, Object> ret = new HashMap<String, Object>();

        for(String property : properties) {
            Object val = JsonParsingUtils.getPropertyFromJsonString(property, allProperties);
            ret.put(property, (val == null) ? "" : val);
        }
        return ret;
    }

    public static Object getDataSourceStatusProperty(String collectionName, int datasourceId, String propertyName) {
        //Properties : num_total, num_robots_denied, num_updated, num_failed, num_not_found, num_unchanged, batch_job,
        // num_new, num_filter_denied, crawl_state, id, message, crawl_stopped, crawl_started, num_deleted, num_access_denied,
        // job_id
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT +
                "/" + datasourceId + "/status";
        String properties = HttpClientUtils.httpGetRequest(url);

        return JsonParsingUtils.getPropertyFromJsonString(propertyName, properties);
    }

    public static Object getDataSourceHistoryProperty(String collectionName, int datasourceId, String propertyName) {
        //Properties :
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT +
                "/" + datasourceId + "/history";
        String properties = HttpClientUtils.httpGetRequest(url);

        JSONArray jsonArray = JSONArray.fromObject(properties);
        if (jsonArray.isEmpty()) {
            return null;
        }
        JSONObject jsonObject = (JSONObject) jsonArray.get(0);
        return jsonObject.get(propertyName);
    }

    public static List<String> getDataSourceNames(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        String properties = HttpClientUtils.httpGetRequest(url);
        return convertObjectListToStringList(JsonParsingUtils.getPropertiesFromDataSourceJson("name", properties));
    }

    public static HashMap<String, Integer> getDataSourceNamesAndIds(String collectionName) {
        HashMap<String,Integer> namesAndIds = new HashMap<String, Integer>();
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        JSONArray jsonArray = JSONArray.fromObject(HttpClientUtils.httpGetRequest(url));
        for(Object o : jsonArray) {
            JSONObject jsonObject = (JSONObject) o;
            namesAndIds.put((String) jsonObject.get("name"), (Integer) jsonObject.get("id"));
        }
        return namesAndIds;
    }

    public static String createDatasource(String collectionName, HashMap<String, Object> properties) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        return HttpClientUtils.httpJsonPostRequest(url, JsonParsingUtils.constructJsonStringFromProperties(properties));
    }

    public static String deleteDatasource(String collectionName, String datasourceId) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT + "/" + datasourceId;

        return HttpClientUtils.httpDeleteRequest(url);
    }
}
