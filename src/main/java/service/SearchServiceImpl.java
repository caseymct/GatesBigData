package service;

import LucidWorksApp.utils.*;
import model.FacetFieldEntry;
import model.FacetFieldEntryList;
import model.SuggestionList;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SearchServiceImpl implements SearchService {

    private static final Logger logger = Logger.getLogger(SearchServiceImpl.class);
    private CoreService coreService;

    @Autowired
    public void setServices(CoreService coreService) {
        this.coreService = coreService;
    }

    private List<String> getFieldNameSubset(Collection<String> fieldNames, boolean isFacetField) {
        List<String> fieldNamesSubset = new ArrayList<String>();

        for(String fieldName : fieldNames) {
            if (SolrUtils.validFieldName(fieldName, isFacetField)) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    private String editFilterQueryDateRange(String fq, String fieldName) {
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

    private String getSolrSuggestion(String coreName, String userInput, String prefixField, String fullField) {

        HashMap<String,String> urlParams = new HashMap<String, String>();
        String queryStr = prefixField + ":" + userInput + "*";
        String[] userInputPieces = userInput.split("%20| ");
        if (userInputPieces.length > 1) {
            //queryStr = "%2B" + prefixField + ":" + StringUtils.join(userInputPieces, "%20%2B" + prefixField + ":");
            queryStr = "+" + prefixField + ":" + StringUtils.join(userInputPieces, " +" + prefixField + ":");
        }

        urlParams.put(Constants.SOLR_QUERY_PARAM, queryStr);
        urlParams.put(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);
        if (fullField.equals(Constants.SOLR_CONTENT_FIELD_NAME)) {
            urlParams.put(Constants.SOLR_FIELDLIST_PARAM, Constants.SOLR_SCORE_FIELD_NAME);
            urlParams.put(Constants.SOLR_HIGHLIGHT_PARAM, "true");
            urlParams.put(Constants.SOLR_HIGHLIGHT_FRAGSIZE_PARAM, "50");
            urlParams.put(Constants.SOLR_HIGHLIGHT_FIELDLIST_PARAM, "content,title");
            urlParams.put(Constants.SOLR_HIGHLIGHT_PRE_PARAM, "<b>");
            urlParams.put(Constants.SOLR_HIGHLIGHT_POST_PARAM, "</b>");
        } else {
            urlParams.put(Constants.SOLR_FIELDLIST_PARAM, fullField + "," + Constants.SOLR_SCORE_FIELD_NAME);
            urlParams.put(Constants.SOLR_GROUP_FIELD_PARAM, fullField);
            urlParams.put(Constants.SOLR_GROUP_SORT_PARAM, Constants.SOLR_GROUP_SORT_DEFAULT);
            urlParams.put(Constants.SOLR_GROUP_PARAM, Constants.SOLR_GROUP_DEFAULT);
        }
        String url = SolrUtils.getSolrSelectURI(coreName, urlParams);
        return HttpClientUtils.httpGetRequest(url);
    }

    private SuggestionList getTopNListings(JSONObject jsonObject, String fullField, int n) {
        SuggestionList suggestionList = new SuggestionList();
        if (fullField.equals(Constants.SOLR_CONTENT_FIELD_NAME)) {
            JSONObject hl = jsonObject.getJSONObject(Constants.SOLR_RESPONSE_HIGHLIGHTING_KEY);
            for(Object keyObj : hl.keySet()) {
                String text  = (String) JsonParsingUtils.extractJSONProperty(hl, Arrays.asList((String) keyObj, Constants.SOLR_CONTENT_FIELD_NAME, "0"), String.class, "");
                String title = (String) JsonParsingUtils.extractJSONProperty(hl, Arrays.asList((String) keyObj, "title", "0"), String.class, "");
                if (!text.equals("") || !title.equals("")) {
                    suggestionList.add(Utils.getUTF8String(text), Constants.SOLR_CONTENT_FIELD_NAME, 1, 1.0);
                }
            }
        } else {
            JSONArray groups = (JSONArray) JsonParsingUtils.extractJSONProperty(jsonObject,
                    Arrays.asList("grouped", fullField, "groups"), JSONArray.class, new JSONArray());

            for(int i = 0; i < groups.size() && suggestionList.size() < n; i++) {
                JSONObject g = groups.getJSONObject(i);
                String text = (String) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "docs", "0", fullField), String.class, null);
                int numFound = (Integer) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "numFound"), Integer.class, Constants.INVALID_INTEGER);
                double score = (Double) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "docs", "0", "score"), Double.class, Constants.INVALID_DOUBLE);
                // TODO: why for test2_data is score a JSONArray and for NA_data score is a double??? mv fields? confused.
                if (score == Constants.INVALID_DOUBLE) {
                    score = (Double) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "docs", "0", "score", "0"), Double.class, Constants.INVALID_DOUBLE);
                }
                if (text != null && numFound != Constants.INVALID_INTEGER && score != Constants.INVALID_DOUBLE) {
                    suggestionList.add(Utils.getUTF8String(text), fullField, numFound, score);
                }
            }
        }
        return suggestionList ;
    }

    public JSONObject suggest(String coreName, String userInput, HashMap<String, String> fieldMap, int listingsPerField) {
        try {
            userInput = URIUtil.encodeQuery(userInput);
        } catch (URIException e) {
            logger.error(e.getMessage());
        }

        JSONObject ret = new JSONObject();
        SuggestionList suggestions = new SuggestionList();

        for(Map.Entry<String, String> entry : fieldMap.entrySet()) {
            String prefixField = entry.getKey();
            String fullField = entry.getValue();
            String response = getSolrSuggestion(coreName, userInput, prefixField, fullField);

            if (!fullField.equals(Constants.SOLR_CONTENT_FIELD_NAME)) {
                try {
                    JSONObject jsonObject = JSONObject.fromObject(response);
                    suggestions.concat(getTopNListings(jsonObject, fullField, listingsPerField));
                } catch (JSONException e) {
                    logger.error(e.getMessage());
                }
            }
        }

        ret.put("suggestions", JsonParsingUtils.convertStringListToJSONArray(suggestions.getSortedFormattedSuggestionList()));
        return ret;
    }

    private FacetFieldEntryList checkFacetFieldEntryList(FacetFieldEntryList facetFields, String coreName) {
        return (facetFields == null || facetFields.size() == 0) ? SolrUtils.getLukeFacetFieldEntryList(coreName) : facetFields;
    }

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer) {
        facetFields = checkFacetFieldEntryList(facetFields, coreName);

        JSONObject response = execQuery(Constants.SOLR_QUERY_DEFAULT, coreName, Constants.SOLR_SORT_FIELD_DEFAULT,
                                        Constants.SOLR_SORT_ORDER_DEFAULT, Constants.SOLR_START_DEFAULT,
                                        Constants.SOLR_ROWS_DEFAULT, null, facetFields, "");

        response = formatFacetResponse(response);
        writer.append(response.toString(1));
    }


    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder, int start, int rows,
                           String fq, FacetFieldEntryList facetFields, String viewFields, List<String> dateFields, StringWriter writer) {
        facetFields = checkFacetFieldEntryList(facetFields, coreName);

        long currTimeMillis = System.currentTimeMillis();
        JSONObject response = execQuery(queryString, coreName, sortType, SolrUtils.getSortOrder(sortOrder), start, rows, fq, facetFields, viewFields);
        response = formatResponse(response, dateFields);
        writer.append(response.toString(1));

        System.out.println("Total search ms: " + (System.currentTimeMillis() - currTimeMillis));
        System.out.flush();
    }


    public JSONObject execQuery(String queryString, String coreName, String sortField, SolrQuery.ORDER sortOrder, int start, int rows,
                                String fq, FacetFieldEntryList facetFields, String viewFields) {

        HashMap<String, String> urlParams = new HashMap<String, String>();
        urlParams.put(Constants.SOLR_QUERY_PARAM, queryString);

        urlParams.put(Constants.SOLR_WT_PARAM, Constants.SOLR_WT_DEFAULT);
        urlParams.put(Constants.SOLR_START_PARAM, "" + start);
        urlParams.put(Constants.SOLR_ROWS_PARAM, "" + rows);
        urlParams.put(Constants.SOLR_INDENT_PARAM, Constants.SOLR_INDENT_DEFAULT);
        urlParams.put(Constants.SOLR_HIGHLIGHT_PARAM, "true");
        urlParams.put(Constants.SOLR_HIGHLIGHT_PRE_PARAM, Constants.SOLR_HIGHLIGHT_PRE_DEFAULT);
        urlParams.put(Constants.SOLR_HIGHLIGHT_POST_PARAM, Constants.SOLR_HIGHLIGHT_POST_DEFAULT);
        urlParams.put(Constants.SOLR_HIGHLIGHT_SNIPPETS_PARAM, Constants.SOLR_HIGHLIGHT_SNIPPETS_DEFAULT);
        if (viewFields != null) {
            urlParams.put(Constants.SOLR_FIELDLIST_PARAM, viewFields);
            urlParams.put(Constants.SOLR_HIGHLIGHT_FIELDLIST_PARAM, viewFields);
        }

        List<String> facetFieldList = new ArrayList<String>();
        if (facetFields != null && facetFields.size() > 0) {
            urlParams.put(Constants.SOLR_FACET_PARAM, Constants.SOLR_FACET_DEFAULT);
            urlParams.put(Constants.SOLR_FACET_MISSING_PARAM, Constants.SOLR_FACET_MISSING_DEFAULT);
            urlParams.put(Constants.SOLR_FACET_LIMIT_PARAM, "" + facetFields.size());

            for(FacetFieldEntry field : facetFields) {
                String fieldName = field.getFieldName();
                String fieldType = field.getFieldType();

                if (fieldType.equals("date") && !field.isMultiValued()) {
                    List<String> dateRange = coreService.getSolrFieldDateRange(coreName, fieldName, DateUtils.SOLR_DATE);
                    urlParams.put(Constants.SOLR_FACET_DATE_FIELD_PARAM, fieldName);
                    urlParams.put(Constants.SOLR_FACET_DATE_START_PARAM, dateRange.get(0));
                    urlParams.put(Constants.SOLR_FACET_DATE_END_PARAM, dateRange.get(1));
                    urlParams.put(Constants.SOLR_FACET_DATE_GAP_PARAM, dateRange.get(2));

                    fq = editFilterQueryDateRange(fq, fieldName);
                } else {
                    facetFieldList.add(fieldName);
                }
            }
        }

        HashMap<String, List<String>> facetFieldMap = new HashMap<String, List<String>>();
        facetFieldMap.put(Constants.SOLR_FACET_FIELD_PARAM, facetFieldList);

        if (fq != null) {
            urlParams.put(Constants.SOLR_FILTERQUERY_PARAM, fq);
        }
        urlParams.put(Constants.SOLR_SORT_PARAM, sortField + " " + sortOrder);

        String url = SolrUtils.getSolrSelectURI(coreName, urlParams, facetFieldMap);
        return JsonParsingUtils.getJSONObject(HttpClientUtils.httpGetRequest(url));
    }

    private SolrDocument errorDoc(String errorMsg) {
        SolrDocument solrDoc = new SolrDocument();
        solrDoc.addField(Constants.SOLR_ERRORSTRING_KEY, errorMsg);
        return solrDoc;
    }

    public SolrDocument getRecord(String coreName, String id) {
        SolrServer server = coreService.getSolrServer(coreName);
        SolrQuery query = new SolrQuery();
        query.setQuery("id:\"" + id + "\"");

        try {
            QueryResponse rsp = server.query(query);
            SolrDocumentList results = rsp.getResults();
            if (results.getNumFound() >= 1) {
                return results.get(0);
            }
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }

        return errorDoc("No document found in core " + coreName + " with ID " + id);
    }

    public void printRecord(String coreName, String id, StringWriter writer) {
        SolrDocument doc = getRecord(coreName, id);
        List<String> fields = new ArrayList<String>(doc.getFieldNames());
        printRecord(doc, writer, fields);
    }

    public void printRecord(String coreName, String id, StringWriter writer, List<String> fields) {
        printRecord(getRecord(coreName, id), writer, fields);
    }

    public void printRecord(SolrDocument doc, StringWriter writer, List<String> fields) {
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);

            g.writeStartObject();
            g.writeObjectFieldStart("Contents");

            for(String s : fields) {
                if (doc.containsKey(s)) {
                    g.writeStringField(s, (String) doc.get(s));
                }
            }

            g.writeEndObject();
            g.writeEndObject();
            g.close();
        } catch (JsonGenerationException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public QueryResponse execQueryOld(String queryString, String coreName, String sortField, SolrQuery.ORDER sortOrder,
                                    int start, int rows, String fq, FacetFieldEntryList facetFields) {

        String url = SolrUtils.getSolrServerURI(coreName);

        try {
            SolrServer server = new HttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.setStart(start);
            query.setRows(rows);

            query.setHighlight(true);
            query.setHighlightSnippets(3);
            query.setHighlightSimplePre("<span class='highlight_text'>");
            query.setHighlightSimplePost("</span>");

            if (facetFields != null && facetFields.size() > 0) {
                query.setFacet(true);
                query.setFacetMissing(true);
                query.setFacetLimit(facetFields.size());

                for(FacetFieldEntry field : facetFields) {
                    String fieldName = field.getFieldName();
                    String fieldType = field.getFieldType();

                    if (fieldType.equals("date") && !field.isMultiValued()) {
                        List<String> dateRange = coreService.getSolrFieldDateRange(coreName, fieldName, DateUtils.SOLR_DATE);
                        query.addDateRangeFacet(fieldName, DateUtils.getDateFromSolrDate(dateRange.get(0)),
                                DateUtils.getDateFromSolrDate(dateRange.get(1)), dateRange.get(2));

                        fq = editFilterQueryDateRange(fq, fieldName);
                    } else {
                        query.addFacetField(fieldName);
                    }
                }
            }

            if (fq != null) {
                query.addFilterQuery(fq);
            }

            if (coreService.isFieldMultiValued(server, sortField)) {
                sortField = "score";
            }
            query.add("wt", "json");
            query.addSortField(sortField, sortOrder);
            return server.query(query);

        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }

        return null;
    }

    private boolean isLocalDirectory(SolrDocument doc) {
        if (doc.containsKey("title")) {
            Object title = doc.get("title");
            String titleContents = "";
            if (title instanceof ArrayList) {
                titleContents = (String) ((ArrayList) doc.get("title")).get(0);
            } else if (title instanceof String) {
                titleContents = (String) title;
            }
            return titleContents.startsWith("Index of /");
        }
        return false;
    }

    private JSONObject formatResponse(JSONObject response, List<String> dateFields) {
        response = JSONObject.fromObject(Utils.getUTF8String(response.toString()));
        response = formatSearchResponse(response, dateFields);
        response = formatFacetResponse(response);
        return response;
    }


    private JSONObject formatSearchResponse(JSONObject response, List<String> dateFields) {

        JSONObject subresponse = response.getJSONObject("response");
        JSONArray docs = (JSONArray) JsonParsingUtils.extractJSONProperty(response, Arrays.asList("response", "docs"), JSONArray.class, new JSONArray());
        for(int i = 0; i < docs.size(); i++) {
            JSONObject doc = docs.getJSONObject(i);
            for(String dateFieldString : dateFields) {
                if (doc.has(dateFieldString)) {
                    doc.put(dateFieldString, DateUtils.getFormattedDateStringFromSolrDate(doc.getString(dateFieldString), DateUtils.LONG_DATE));
                }
            }
            docs.set(i, doc);
        }
        subresponse.put("docs", docs);
        response.put("response", subresponse);
        return response;
    }

    private JSONObject formatFacetResponse(JSONObject response) {
        JSONObject facetJsonObj = (JSONObject) response.remove(Constants.SOLR_RESPONSE_FACET_KEY);
        JSONObject facetFields  = (JSONObject) JsonParsingUtils.extractJSONProperty(facetJsonObj, Arrays.asList(Constants.SOLR_RESPONSE_FACET_FIELDS_KEY),
                                                                                    JSONObject.class, new JSONObject());
        JSONObject facetDates   = (JSONObject) JsonParsingUtils.extractJSONProperty(facetJsonObj, Arrays.asList(Constants.SOLR_RESPONSE_FACET_DATES_KEY),
                JSONObject.class, new JSONObject());

        JSONObject facetResponse = new JSONObject();

        for(Object field : facetFields.keySet()) {
            String name = (String) field;
            if (name.endsWith(".facet")) {
                name = name.substring(0, name.lastIndexOf(".facet"));
            }
            JSONArray values = facetFields.getJSONArray((String) field);
            for(int i = 0; i < values.size(); i++) {
                String facetName = Utils.getUTF8String(values.getString(i++));
                int facetCount = values.getInt(i);
                if (facetCount > 0) {
                    facetResponse.accumulate(name, facetName + " (" + facetCount + ")");
                }
            }
        }

        for(Object field : facetDates.keySet()) {
            String name = (String) field;

            JSONObject fieldFacetDates = facetDates.getJSONObject(name);
            String gap = fieldFacetDates.getString("gap");

            for(Object dateObj : fieldFacetDates.keySet()) {
                String fieldFacetDateKey = (String) dateObj;
                if (fieldFacetDateKey.equals("gap") || fieldFacetDateKey.equals("start") ||
                        fieldFacetDateKey.equals("end")) continue;

                int facetCount = fieldFacetDates.getInt(fieldFacetDateKey);
                if (facetCount > 0) {
                    facetResponse.accumulate(name, DateUtils.getDateStringFromSolrDate(fieldFacetDateKey) + " - " +
                                                   DateUtils.solrDateMath(fieldFacetDateKey, gap) + " (" + facetCount + ")");
                }
            }
        }
        response.accumulate("facets", facetResponse);
        return response;
    }

}
