package LucidWorksApp.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
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

    public static String getDataSourceProperties(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        return HttpClientUtils.httpGetRequest(url);
    }

    public static Object getDataSourceProperty(String collectionName, int datasourceId, String propertyName) {
        //Properties : commit_on_finish, verify_access, sql_select_statement, mapping, collection, type,
        // password, url, crawler, nested_queries, id, username, category, delta_sql_query, name, commit_within,
        // primary_key, driver, max_docs
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT + "/" + datasourceId;
        String properties = HttpClientUtils.httpGetRequest(url);


        return JsonParsingUtils.getPropertyFromJsonString(propertyName, properties);
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

    public static List<String> getExistingNamesAndDatasources(String collectionName, String datasourceCategory) {
        List<String> namesAndDatasources = new ArrayList<String>();
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        JSONArray jsonArray = JSONArray.fromObject(HttpClientUtils.httpGetRequest(url));
        for(Object o : jsonArray) {
            JSONObject jsonObject = (JSONObject) o;
            String category = (String) jsonObject.get("category");

            if (category.equals(datasourceCategory)) {
                namesAndDatasources.add((String) jsonObject.get("name"));

                if (category.equals("Web") || category.equals("Jdbc")) {
                    String u = (String) jsonObject.get("url");
                    if (u.endsWith("/")) {
                        u = u.substring(0, u.length()-1);
                    }
                    namesAndDatasources.add(u);
                } else if (category.equals("Filesystem")) {
                    namesAndDatasources.add((String) jsonObject.get("path"));
                }
            }
        }
        return namesAndDatasources;
    }

    public static String createDatasource(String collectionName, HashMap<String, Object> properties) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        // is there a way to get a better error message off the server?
        /*
        List<String> existingNamesAndDatasources = getExistingNamesAndDatasources(collectionName, (String) properties.get("category"));
        if (existingNamesAndDatasources.contains((String) properties.get("name"))) {
            return "ERROR: Datasource with this name already exists";
        }
        if (existingNamesAndDatasources.contains((String) properties.get("url"))) {
            return "ERROR: Datasource with this uri already exists";
        }
        */
        return HttpClientUtils.httpPostRequest(url, JsonParsingUtils.constructJsonStringFromProperties(properties));
    }
}
