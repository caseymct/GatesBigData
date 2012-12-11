package LucidWorksApp.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.solr.common.util.NamedList;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;

import java.io.IOException;
import java.util.*;


public class JsonParsingUtils {

    public static final String JSON_PARSE_ERROR_KEY = "JSON_PARSE_ERROR";

    public static boolean isJsonErrorObject(JSONObject jsonObject) {
        return jsonObject.has(JSON_PARSE_ERROR_KEY);
    }

    public static JSONObject getJSONObject(String object) {
        try {
            return JSONObject.fromObject(object);
        } catch (JSONException e) {
            JSONObject error = new JSONObject();
            error.put(JSON_PARSE_ERROR_KEY, true);
            return error;
        }
    }

    public static List<String> convertJSONArrayToStringList(JSONArray jsonArray) {
        List<String> s = new ArrayList<String>();

        for(int i = 0; i < jsonArray.size(); i++) {
            s.add(jsonArray.getString(i));
        }
        return s;
    }

    public static Object extractJSONProperty(JSONObject jsonObject, List<String> fields, Class expectedReturnClass,
                                             Object defaultReturnValue) {
        Object currObject = jsonObject;

        for(int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (currObject instanceof JSONObject && ((JSONObject) currObject).has(field)) {
                currObject = ((JSONObject) currObject).get(field);
            } else if (currObject instanceof JSONArray) {
                int index = Utils.getInteger(field);
                if (index >= 0 && index < ((JSONArray) currObject).size()) {
                    currObject = ((JSONArray) currObject).get(index);
                }
            } else if (i < fields.size()) {
                return defaultReturnValue;
            }
        }

        try {
            Class currObjClass = currObject.getClass();
            return (currObjClass != null && currObjClass.equals(expectedReturnClass)) ? currObject : defaultReturnValue;
        } catch (NullPointerException e) {
            return defaultReturnValue;
        }
    }

    public static JSONArray convertStringListToJSONArray(List<String> l) {
        JSONArray ret = new JSONArray();
        for(String s : l) {
            ret.add(s);
        }
        return ret;
    }

    public static Object getPropertyFromJsonString(String propertyName, String jsonString) {
        try {
            JSONObject jsonObject = JSONObject.fromObject(jsonString);
            if (jsonObject.has(propertyName)) {
                return jsonObject.get(propertyName);
            }
        } catch (JSONException e) {
            System.out.println(e.getMessage());
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

    public static void printJSONObject(JSONObject jsonObject, String objectFieldName, String fullPath,
                                       List<String> previewFields, JsonGenerator g) throws IOException {
        g.writeObjectFieldStart(objectFieldName);

        for(Object key : jsonObject.names()) {
            Object value = jsonObject.get(key);
            String newFullPath = fullPath.equals("") ? (String) key : fullPath + "." + key;

            if (value instanceof JSONArray) {
                g.writeArrayFieldStart((String) key);

                for (Object o : (JSONArray) value) {
                    if (o instanceof JSONObject && (previewFields == null || previewFields.contains(newFullPath))) {
                        printJSONObject((JSONObject) o, (String) key, newFullPath, previewFields, g);
                    } else {
                        Utils.writeValueByType(value, g);
                    }
                }
                g.writeEndArray();

            } else if (value instanceof JSONObject) {
                if (previewFields == null || previewFields.contains(newFullPath)) {
                    printJSONObject((JSONObject) value, (String) key, newFullPath, previewFields, g);
                }

            } else {
                if (previewFields == null || previewFields.contains(newFullPath)) {
                    Utils.writeValueByType((String) key, value, g);
                }
            }
        }
        g.writeEndObject();
    }

}
