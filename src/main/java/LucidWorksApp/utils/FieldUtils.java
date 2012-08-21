package LucidWorksApp.utils;

import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldUtils extends Utils {

    private static String CSVFILESUPLOADED_FIELDNAME = "csvFilesUploaded";
    private static String FIELDS_ENDPOINT = "/fields";


    public static String updateCSVFilesUploadedField(String collectionName, String fileName, boolean onServer) {
        String postUrl = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT;
        String putUrl = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT + "/" +
                CSVFILESUPLOADED_FIELDNAME;
        JSONObject field = new JSONObject();

        fileName = (onServer ? "server:" : "localsystem:") + fileName;

        if (!fieldExists(CSVFILESUPLOADED_FIELDNAME, collectionName)) {
            field.put("name", CSVFILESUPLOADED_FIELDNAME);
            field.put("default_value", fileName);
            field.put("indexed", "false");
            field.put("stored", "false");
            field.put("field_type", "string");
            field.put("short_field_boost", "moderate");

            return HttpClientUtils.httpJsonPostRequest(postUrl, field.toString());

        } else {
            String filesUploadedStr = (String) getFieldValue(CSVFILESUPLOADED_FIELDNAME, collectionName);
            List<String> filesUploaded = new ArrayList<String>();

            if (!filesUploadedStr.equals("")) {
                filesUploaded = new ArrayList<String>(Arrays.asList(filesUploadedStr.split(";")));
                if (filesUploaded.contains(fileName)) {
                    return "";
                }
            }

            filesUploaded.add(fileName);
            field.put("default_value", StringUtils.join(filesUploaded, ";"));
            return HttpClientUtils.httpJsonPutRequest(putUrl, field.toString());
        }
    }

    public static String clearCSVFilesUploadedField(String collectionName) {
        String putUrl = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT + "/" +
                CSVFILESUPLOADED_FIELDNAME;
        JSONObject field = new JSONObject();
        field.put("default_value", "");
        return HttpClientUtils.httpJsonPutRequest(putUrl, field.toString());
    }

    public static String removeEntryFromCSVFilesUploadedField(String collectionName, String fileName) {
        String putUrl = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT + "/" +
                CSVFILESUPLOADED_FIELDNAME;

        List<String> filesUploaded = new ArrayList<String>(Arrays.asList(((String) getFieldValue(CSVFILESUPLOADED_FIELDNAME, collectionName)).split(";")));
        if (filesUploaded.contains(fileName)) {
            filesUploaded.remove(fileName);

            JSONObject field = new JSONObject();
            field.put("default_value", StringUtils.join(filesUploaded, ";"));
            return HttpClientUtils.httpJsonPutRequest(putUrl, field.toString());
        }

        return "{ \"error\" : \"Files uploaded field does not contain file name\"}";
    }

    public static String deletedField(String collectionName, String fieldName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT + "/" + fieldName;

        if (fieldExists(fieldName, collectionName)) {
            return HttpClientUtils.httpDeleteRequest(url);
        }

        return "{ \"errors\" : {\"Field does not exist\"}";
    }

    public static String createCSVField(String collectionName, String fieldName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + "/fields";

        if (!fieldExists(fieldName, collectionName)) {
            JSONObject field = new JSONObject();
            field.put("name", fieldName);
            field.put("copy_fields", fieldName + "_display");
            field.put("multi_valued", "false");
            field.put("field_type", "string");
            field.put("indexed", "true");
            field.put("stored", "true");

            HttpClientUtils.httpJsonPostRequest(url, field.toString());

            JSONObject displayField = new JSONObject();
            displayField.put("name", fieldName + "_display");
            displayField.put("facet", "true");
            displayField.put("multi_valued", "false");
            displayField.put("field_type", "string");
            displayField.put("indexed", "true");
            displayField.put("stored", "false");

            HttpClientUtils.httpJsonPostRequest(url, displayField.toString());
        }
        return "";
    }

    public static Object getFieldValue(String fieldName, String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT + "/" + fieldName +
                "?wt=json";
        JSONObject jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url));

        if (jsonObject.containsKey("default_value")) {
            Object value = jsonObject.get("default_value");
            return value instanceof JSONNull ? "" : value;
        }
        return null;
    }

    public static String csvFilesImported(String collectionName) {
        if (fieldExists(CSVFILESUPLOADED_FIELDNAME, collectionName)) {
            Object fieldValue = getFieldValue(CSVFILESUPLOADED_FIELDNAME, collectionName);
            return (fieldValue == null || fieldValue instanceof JSONNull) ? "" : (String) fieldValue;
        }
        return "";
    }

    public static boolean fieldExists(String fieldName, String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + FIELDS_ENDPOINT + "/" + fieldName +
                "?wt=json";
        JSONObject jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url));

        return !(jsonObject.containsKey("http_status_code") && jsonObject.get("http_status_code").equals(404));
    }

    public static List<String> getFieldNames(String collectionName) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + "/fields";
        List<String> fieldNames = new ArrayList<String>();

        JSONArray fields = JSONArray.fromObject(HttpClientUtils.httpGetRequest(url));
        for(int i = 0; i < fields.size(); i++) {
            JSONObject jsonObject = fields.getJSONObject(i);
            fieldNames.add((String) jsonObject.get("name"));
        }

        return fieldNames;
    }
}
