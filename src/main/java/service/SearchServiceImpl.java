package service;

import LucidWorksApp.utils.*;
import model.FacetFieldEntry;
import model.FacetFieldEntryList;
import model.SuggestionList;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SearchServiceImpl implements SearchService {

    private String SOLR_DEFAULT_SORT_FIELD          = "score";
    private String SOLR_DEFAULT_QUERY               = "*:*";
    private int SOLR_DEFAULT_START                  = 0;
    private int SOLR_DEFAULT_ROWS                   = 10;
    private SolrQuery.ORDER SOLR_DEFAULT_SORT_ORDER = SolrQuery.ORDER.asc;

    private static final Logger logger = Logger.getLogger(SearchServiceImpl.class);

    private CoreService coreService;

    @Autowired
    public void setServices(CoreService coreService) {
        this.coreService = coreService;
    }

    public SolrQuery.ORDER getSortOrder(String sortOrder) {
        return sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
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
            queryStr = "%2B" + prefixField + ":" + StringUtils.join(userInputPieces, "%20%2B" + prefixField + ":");
        }
        urlParams.put("q", queryStr);
        urlParams.put("wt", "json");
        urlParams.put("fl", fullField + ",score");
        urlParams.put("group.field", fullField);
        urlParams.put("group.sort", "score%20desc");
        urlParams.put("group", "true");

        String url = SolrUtils.getSolrSelectURI(coreName, urlParams);
        return HttpClientUtils.httpGetRequest(url);
    }

    private SuggestionList getTopNListings(JSONObject jsonObject, String fullField, int n) {
        SuggestionList suggestionList = new SuggestionList();
        JSONArray groups = (JSONArray) JsonParsingUtils.extractJSONProperty(jsonObject,
                Arrays.asList("grouped", fullField, "groups"), JSONArray.class, new JSONArray());

        for(int i = 0; i < groups.size() && suggestionList.size() < n; i++) {
            JSONObject g = groups.getJSONObject(i);
            String text = (String) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "docs", "0", fullField), String.class, null);
            int numFound = (Integer) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "numFound"), Integer.class, Utils.INVALID_INTEGER);
            double score = (Double) JsonParsingUtils.extractJSONProperty(g, Arrays.asList("doclist", "docs", "0", "score"), Double.class, Utils.INVALID_DOUBLE);

            if (text != null && numFound != Utils.INVALID_INTEGER && score != Utils.INVALID_DOUBLE) {
                suggestionList.add(Utils.getUTF8String(text), fullField, numFound, score);
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

            try {
                JSONObject jsonObject = JSONObject.fromObject(response);
                suggestions.concat(getTopNListings(jsonObject, fullField, listingsPerField));
            } catch (JSONException e) {
                logger.error(e.getMessage());
            }
        }

        ret.put("suggestions", JsonParsingUtils.convertStringListToJSONArray(suggestions.getSortedFormattedSuggestionList()));
        return ret;
    }

    private FacetFieldEntryList checkFacetFieldEntryList(FacetFieldEntryList facetFields, String coreName) {
        if (facetFields == null || facetFields.size() == 0) {
            facetFields = SolrUtils.getLukeFacetFieldEntryList(coreName);
        }
        return facetFields;
    }

    public void writeFacets(String coreName, FacetFieldEntryList facetFields, StringWriter writer) {
        facetFields = checkFacetFieldEntryList(facetFields, coreName);

        try {
            QueryResponse rsp = execQuery(SOLR_DEFAULT_QUERY, coreName, SOLR_DEFAULT_SORT_FIELD, SOLR_DEFAULT_SORT_ORDER,
                    SOLR_DEFAULT_START, SOLR_DEFAULT_ROWS, null, facetFields);
            writeResponse(false, rsp, 0, writer);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, int rows, String fq, FacetFieldEntryList facetFields, StringWriter writer) {
        facetFields = checkFacetFieldEntryList(facetFields, coreName);

        try {
            QueryResponse rsp = execQuery(queryString, coreName, sortType, getSortOrder(sortOrder), start, rows, fq, facetFields);
            writeResponse(true, rsp, start, writer);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void writeResponse(boolean includeSearchResults, QueryResponse rsp, int start, StringWriter writer) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeObjectFieldStart("response");

        if (includeSearchResults) {
            g.writeNumberField("start", start);
            writeSearchResponse(rsp, g);
        }
        writeFacetResponse(rsp, g);
        g.writeEndObject();
        g.close();
    }

    public QueryResponse execQuery(String queryString, String coreName, String sortField, SolrQuery.ORDER sortOrder,
                                    int start, int rows, String fq, FacetFieldEntryList facetFields) {

        String url = SolrUtils.getSolrServerURI(coreName);

        try {
            SolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.setStart(start);
            query.setRows(rows);

            if (facetFields != null && facetFields.size() > 0) {
                query.add("facet", "true");
                query.add("facet.method", "enum");
                query.add("facet.missing", "true");
                query.setFacetLimit(facetFields.size());

                for(FacetFieldEntry field : facetFields) {
                    String fieldName = field.getFieldName();
                    String fieldType = field.getFieldType();

                    if (fieldType.equals("date") && !field.isMultiValued()) {
                        List<String> dateRange = coreService.getSolrFieldDateRange(coreName, fieldName, DateUtils.SOLR_DATE);
                        query.add("facet.date", fieldName);
                        query.add("facet.date.start", dateRange.get(0));
                        query.add("facet.date.end", dateRange.get(1));
                        query.add("facet.date.gap", dateRange.get(2));

                        fq = editFilterQueryDateRange(fq, fieldName);
                    } else {
                        query.add("facet.field", fieldName);
                    }
                }
            }

            if (fq != null) {
                query.addFilterQuery(fq);
            }

            if (coreService.isFieldMultiValued(server, sortField)) {
                sortField = "score";
            }

            query.addSortField(sortField, sortOrder);
            return server.query(query);

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
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

    private void writeSearchResponse(QueryResponse rsp, JsonGenerator g) throws IOException {
        SolrDocumentList docs = rsp.getResults();

        g.writeNumberField("numFound", docs.getNumFound());
        g.writeArrayFieldStart("docs");

        for (SolrDocument doc : docs) {
            if (!isLocalDirectory(doc)) {
                g.writeStartObject();
                for(String fieldName : getFieldNameSubset(doc.getFieldNames(), false)) {
                    String[] subFields = StringUtils.split(fieldName, ".");
                    if (subFields.length > 1) {
                        g.writeObjectFieldStart(subFields[0]);
                        Utils.writeValueByType(subFields[1], doc.getFieldValue(fieldName), g);
                        g.writeEndObject();
                    } else {
                        Utils.writeValueByType(fieldName, doc.getFieldValue(fieldName), g);
                    }
                }
                g.writeEndObject();
            }
        }
        g.writeEndArray();
    }

    private void writeFacetResponse(QueryResponse rsp, JsonGenerator g) throws IOException {

        g.writeArrayFieldStart("facets");
        for(FacetField facetField : rsp.getFacetFields()) {
            if (facetField.getValues() == null) continue;

            g.writeStartObject();
            String name = facetField.getName();
            if (name.endsWith(".facet")) {
                name = name.substring(0, name.lastIndexOf(".facet"));
            }
            g.writeStringField("name", name);

            g.writeArrayFieldStart("values");
            for(FacetField.Count count : facetField.getValues()) {
                if (count.getCount() > 0) {
                    g.writeString(count.getName() + " (" + count.getCount() + ")");
                }
            }
            g.writeEndArray();
            g.writeEndObject();
        }

        if (rsp.getFacetDates().size() >= 1 && rsp.getFacetDates().get(0).getValues() != null) {
            for(FacetField facetField : rsp.getFacetDates()) {
                List<FacetField.Count> facetFieldValues = facetField.getValues();
                if (facetFieldValues != null) {
                    g.writeStartObject();
                    g.writeStringField("name", facetField.getName());

                    g.writeArrayFieldStart("values");
                    for(FacetField.Count count : facetFieldValues) {
                        if (count.getCount() > 0) {
                            g.writeString(DateUtils.getDateStringFromSolrDate(count.getName()) + " - " +
                                DateUtils.solrDateMath(count.getName(), count.getFacetField().getGap())
                                + " (" + count.getCount() + ")");
                        }
                    }
                    g.writeEndArray();
                    g.writeEndObject();
                }
            }
        }
        g.writeEndArray();
    }
}
