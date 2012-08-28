package LucidWorksApp.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.common.util.NamedList;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: caseymctaggart
 * Date: 7/26/12
 * Time: 2:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class JsonParsingUtils {


    public static Object getContingentPropertyFromDataSourceJson(String dataSourcePropertyName, String dataSourcePropertyValue,
                                                       String propertyToRetrieveName, String dataSourceFullJson) {

        JSONObject dataSourcesJsonObject = JSONObject.fromObject("{ obj : " + dataSourceFullJson + "}");
        JSONArray jsonArray = (JSONArray) dataSourcesJsonObject.get("obj");

        for(Object o : jsonArray) {
            JSONObject dataSourceJsonObject = (JSONObject) o;
            if (dataSourceJsonObject.has(dataSourcePropertyName) &&
                    dataSourceJsonObject.get(dataSourcePropertyName).equals(dataSourcePropertyValue)) {
                return dataSourceJsonObject.get(propertyToRetrieveName);
            }
        }

        return "";
    }

    public static List<Object> getPropertiesFromDataSourceJson(String propertyName, String dataSourceFullJson) {

        List<Object> properties = new ArrayList<Object>();

        //JSONObject dataSourceJsonObject = JSONObject.fromObject("{ obj : " + dataSourceFullJson + "}");

        //if (dataSourceJsonObject.get("obj") instanceof JSONObject) {
        if (dataSourceFullJson.startsWith("{")) {
            JSONObject jsonObject = JSONObject.fromObject(dataSourceFullJson);
            //JSONObject jsonObject = (JSONObject) dataSourceJsonObject.get("obj");
            if (jsonObject.has(propertyName)) {
                properties.add(jsonObject.get(propertyName));
            }
        //} else if (dataSourceJsonObject.get("obj") instanceof JSONArray) {
        } else if (dataSourceFullJson.startsWith("[")) {

            //for (Object o : (JSONArray) dataSourceJsonObject.get("obj")) {
            for(Object o : JSONArray.fromObject(dataSourceFullJson)) {
                JSONObject jsonObject = (JSONObject) o;
                if (jsonObject.has(propertyName)) {
                    properties.add(jsonObject.get(propertyName));
                }
            }
        }

        return properties;
    }

    public static Object getPropertyFromJsonString(String propertyName, String jsonString) {
        JSONObject jsonObject = JSONObject.fromObject(jsonString);

        if (jsonObject.has(propertyName)) {
            return jsonObject.get(propertyName);
        }

        return null;
    }

    public static String constructJsonStringFromProperties(HashMap<String, Object> properties) {
        JSONObject jsonObject = new JSONObject();
        for(Map.Entry<String, Object> entry : properties.entrySet()) {
            jsonObject.put(entry.getKey(), entry.getValue().toString());
        }
        return jsonObject.toString();
    }

    private static JSONObject recursivelyConstructJSON(Map.Entry entry, JSONObject json)  {
        if (entry.getValue() instanceof NamedList) {
            JSONObject subJson = new JSONObject();
            for(Object o : (NamedList) entry.getValue()) {
                subJson = recursivelyConstructJSON((Map.Entry) o, subJson);
            }
            json.put(entry.getKey(), subJson);
        } else {
            json.put(entry.getKey(), entry.getValue().toString());
        }
        return json;
    }

    public static JSONObject constructJSONObjectFromNamedList(NamedList namedList) {
        JSONObject json = new JSONObject();

        for(Object o : namedList) {
            json = recursivelyConstructJSON((Map.Entry) o, json);
        }
        return json;
    }
}
