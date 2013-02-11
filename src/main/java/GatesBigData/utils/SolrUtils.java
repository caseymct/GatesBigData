package GatesBigData.utils;

import model.FacetFieldEntryList;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolrUtils {

    public static final String SOLR_ENDPOINT         = "solr";
    public static final String SOLR_SUGGEST_ENDPOINT = "suggest";
    public static final String SOLR_SELECT_ENDPOINT  = "select";
    public static final String UPDATECSV_ENDPOINT    = "update/csv";


    public static final String SOLR_SCHEMA_HDFSKEY              = "HDFSKey";
    public static final String SOLR_SCHEMA_HDFSSEGMENT          = "HDFSSegment";
    public static final String SOLR_SCHEMA_PREFIXFIELD_ENDSWITH = "Prefix";

    static Pattern FIELDNAMES_TOIGNORE = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
            "HDFSKey|boost|digest|host|segment|tstamp).*");


    public static String stripHighlightHtml(String s, String hlPre, String hlPost) {
        return s.replaceAll(hlPre + "|" + hlPost, "");
    }

    public static double getDoubleValueIfExists(Map<String, Double> map, String key) {
        return map.containsKey(key) ? map.get(key) : 0.0;
    }

    public static boolean validFieldName(String fieldName, boolean isFacetField) {
        Matcher m = FIELDNAMES_TOIGNORE.matcher(fieldName);
        return (!isFacetField && fieldName.equals(SOLR_SCHEMA_HDFSKEY)) || !m.matches();
    }

    public static String getSolrServerURI(String coreName) {
        return Utils.addToUrlIfNotEmpty(Constants.SOLR_SERVER + "/" + SOLR_ENDPOINT, coreName);
    }

    public static String getSolrSuggestURI(String fieldSpecificEndpoint, String coreName, HashMap<String,String> urlParams) {
        String uri = getSolrServerURI(coreName) + "/" + SOLR_SUGGEST_ENDPOINT;
        return Utils.addToUrlIfNotEmpty(uri, fieldSpecificEndpoint) + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrSelectURI(String coreName) {
        return new Path(getSolrServerURI(coreName), SOLR_SELECT_ENDPOINT).toString();
    }

    public static String getSolrSelectURI(String coreName, HashMap<String,String> urlParams) {
        return getSolrSelectURI(coreName) + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrSelectURI(String coreName, HashMap<String,String> urlParams, HashMap<String, List<String>> repeatKeyUrlParams) {
        return getSolrSelectURI(coreName) + Utils.constructUrlParams(urlParams, repeatKeyUrlParams);
    }

    public static String getUpdateCsvEndpoint(String coreName, HashMap<String,String> urlParams) {
        return getSolrServerURI(coreName) + "/" + UPDATECSV_ENDPOINT + Utils.constructUrlParams(urlParams);
    }

    public static String getSolrCreateCollectionURI(String coreName, String name, int numShards, int replicationFactor) {
        HashMap<String, String> urlParams = new HashMap<String, String>();
        urlParams.put("action", "CREATE");
        urlParams.put("name", name);
        urlParams.put("numShards", numShards + "");
        urlParams.put("replicationFactor", replicationFactor + "");

        return getSolrServerURI(coreName) + "/admin/collections" + Utils.constructUrlParams(urlParams);
    }

    public static SolrQuery.ORDER getSortOrder(String sortOrder) {
        return sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }

    public static String getDocumentContentType(SolrDocument doc) {
        return getFieldValue(doc, Constants.SOLR_CONTENT_TYPE_FIELD_NAME, Constants.TEXT_CONTENT_TYPE);
    }

    public static String getFieldValue(SolrDocument doc, String fieldName, String defaultValue) {
        if (doc != null && doc.containsKey(fieldName)) {
            return (String) doc.get(fieldName);
        }
        return defaultValue;
    }

    public static boolean shouldHaveThumbnail(String fileName) {
        return !Utils.fileHasExtension(fileName, Constants.JSON_FILE_EXT);
    }

    public static SolrQuery setResponseFormatAsJSON(SolrQuery query) {
        query.add(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);
        return query;
    }

    public static boolean isLocalDirectory(SolrDocument doc) {
        if (doc.containsKey(Constants.SOLR_TITLE_FIELD_NAME)) {
            Object title = doc.get(Constants.SOLR_TITLE_FIELD_NAME);
            String titleContents = "";
            if (title instanceof ArrayList) {
                titleContents = (String) ((ArrayList) doc.get(Constants.SOLR_TITLE_FIELD_NAME)).get(0);
            } else if (title instanceof String) {
                titleContents = (String) title;
            }
            return titleContents.startsWith("Index of /");
        }
        return false;
    }

    public static String editFilterQueryDateRange(String fq, String fieldName) {
        if (fq == null || !fq.contains(fieldName)) return fq;
        Pattern p = Pattern.compile(".*\\+" + fieldName + ":([\\(|\\[])([^\\)|\\]]*)([\\)|\\]])(\\+.*)*");
        Matcher m = p.matcher(fq);
        if (m.matches()) {
            String newDateFq = "";
            String[] newDates = m.group(2).split("\" \"");

            for(String d : newDates) {
                String[] dates = d.replaceAll("^\"|\"$", "").split("\\s(-|TO|to)\\s");
                newDateFq += fieldName + ":[" + DateUtils.getSolrDate(dates[0]) + " TO " + DateUtils.getSolrDate(dates[1]) + "] ";
            }
            // If it's just one date, then use the + to indicate AND
            if (newDates.length == 1) {
                newDateFq = "+" + newDateFq;
            }
            fq = fq.replace("+" + fieldName + ":" + m.group(1) + m.group(2) + m.group(3), newDateFq);
        }
        return fq;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(SolrDocumentList docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for (SolrDocument doc : docs) {
            String hdfsSeg = (String) doc.getFieldValue(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add((String) doc.getFieldValue(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }

    public static FacetFieldEntryList constructFacetFieldEntryList(String facetFieldInfo, SolrCollectionSchemaInfo schemaInfo) {
        FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();

        if (Utils.nullOrEmpty(facetFieldInfo)) {
            return schemaInfo.getFacetFieldEntryList();
        } else {
            List<String> lukeFacetFieldNames = schemaInfo.getFacetFieldNames();

            for(String ret : facetFieldInfo.split(",")) {
                String[] n = ret.split(":");

                if (lukeFacetFieldNames.contains(n[0])) {
                    facetFieldEntryList.add(n[0], n[1], n[2]);
                }
            }
        }

        return facetFieldEntryList;
    }

    public static HashMap<String, List<String>> getSegmentToFilesMap(JSONArray docs) {
        HashMap<String, List<String>> segToFileMap = new HashMap<String, List<String>>();

        for(int i = 0; i < docs.size(); i++) {
            JSONObject doc = docs.getJSONObject(i);
            String hdfsSeg = doc.getString(SOLR_SCHEMA_HDFSSEGMENT);
            List<String> fileNames = segToFileMap.containsKey(hdfsSeg) ? segToFileMap.get(hdfsSeg) : new ArrayList<String>();
            fileNames.add(doc.getString(SOLR_SCHEMA_HDFSKEY));
            segToFileMap.put(hdfsSeg, fileNames);
        }
        return segToFileMap;
    }
}
