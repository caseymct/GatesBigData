package GatesBigData.utils;

import GatesBigData.constants.Constants;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;


public class JSONUtils {

    public static <T> Object invokeMethod(T t, String methodName) {
        if (t == null) return null;

        Method m;
        try {
            m = t.getClass().getDeclaredMethod(methodName);
        } catch (NoSuchMethodException e) {
            m = null;
        }
        if (m != null) {
            m.setAccessible(true);
            try {
                return m.invoke(t);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static boolean validField(JSONObject contents, String field) {
        return !Utils.nullOrEmpty(contents) && field != null && contents.containsKey(field);
    }

    public static boolean validField(JSONObject contents, String field, Class c) {
        return !Utils.nullOrEmpty(contents) && field != null && contents.containsKey(field) && c.isInstance(contents.get(field));
    }

    public static JSONObject convertStringToJSONObject(String contents) {
        if (Utils.nullOrEmpty(contents)) return null;

        try {
            return JSONObject.fromObject(contents);
        } catch (JSONException e) {
            return null;
        }
    }

    public static List<String> getStringListFromFieldString(JSONObject o, String field) {
        return convertJSONArrayToStringList(getJSONArrayValue(o, field));
    }

    public static JSONObject getJSONArrayEntry(JSONArray jsonArray, int i) {
        try {
            return jsonArray.getJSONObject(i);
        } catch (JSONException e) {
            return null;
        }
    }

    public static JSONArray convertStringToJSONArray(String contents) {
        if (Utils.nullOrEmpty(contents)) return new JSONArray();

        try {
            return JSONArray.fromObject(contents);
        } catch (JSONException e) {
            return null;
        }
    }

    public static JSONArray convertCollectionToJSONArray(Collection c) {
        JSONArray ret = new JSONArray();
        for(Object s : c) {
            ret.add(s);
        }
        return ret;
    }

    public static List<String> convertJSONArrayToStringList(JSONArray jsonArray) {
        List<String> s = new ArrayList<String>();

        if (jsonArray != null) {
            for(int i = 0; i < jsonArray.size(); i++) {
                s.add(jsonArray.getString(i));
            }
        }
        return s;
    }

    public static String convertJSONArrayToDelimitedString(JSONArray jsonArray) {
        return convertJSONArrayToDelimitedString(jsonArray, Constants.DEFAULT_DELIMETER);
    }

    public static String convertJSONArrayToDelimitedString(JSONArray jsonArray, String delimiter) {
        if (Utils.nullOrEmpty(delimiter)) delimiter = Constants.DEFAULT_DELIMETER;

        return !Utils.nullOrEmpty(jsonArray) ? StringUtils.join(convertJSONArrayToStringList(jsonArray), delimiter) : null;
    }

    public static JSONArray getJSONArrayValue(JSONObject contents, String field) {
        return validField(contents, field, JSONArray.class) ? contents.getJSONArray(field) : null;
    }

    public static JSONArray getJSONArrayValue(JSONObject contents, String field, JSONArray defaultValue) {
        return validField(contents, field, JSONArray.class) ? contents.getJSONArray(field) : defaultValue;
    }

    public static JSONObject getJSONObjectValue(JSONObject contents, String field) {
        return validField(contents, field, JSONObject.class) ? contents.getJSONObject(field) : null;
    }

    public static JSONObject getJSONObjectValue(JSONArray contents, int i) {
        Object o = i < contents.size() ? contents.get(i) : null;
        return o instanceof JSONObject ? contents.getJSONObject(i) : null;
    }

    public static String getStringValue(JSONObject contents, String field) {
        return validField(contents, field) ? contents.getString(field) : null;
    }

    public static HashMap<String, String> convertJSONObjectToHashMap(JSONObject jsonObject) {
        HashMap<String, String> s = new HashMap<String, String>();

        for(Object nameObject : jsonObject.names()) {
            s.put(nameObject.toString(), jsonObject.getString(nameObject.toString()));
        }
        return s;
    }

    public static Object extractJSONProperty(JSONObject jsonObject, List<String> fields, Class expectedReturnClass,
                                             Object defaultReturnValue) {
        if (Utils.nullOrEmpty(jsonObject)) {
            return null;
        }

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
