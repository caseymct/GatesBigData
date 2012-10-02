package service;

import LucidWorksApp.utils.DateUtils;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.schema.DateField;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SearchServiceImpl implements SearchService {

    private Pattern fieldNamesToIgnore;
    /*= Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
                                                                 Utils.getSolrSchemaHdfskey() +
                                                                "|boost|digest|host|segment|tstamp).*|.*id");
      */
    private SolrService solrService;

    @Autowired
    public void setServices(SolrService solrService) {
        this.solrService = solrService;
        fieldNamesToIgnore = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
                solrService.getSolrSchemaHDFSKey() +
                "|boost|digest|host|segment|tstamp).*|.*id");
    }

    public List<String> getSolrIndexDateRange(String collectionName) {

        int buckets = 10;
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("q", "timestamp:*");
        urlParams.put("rows", "1");
        urlParams.put("wt", "json");
        urlParams.put("fl", "timestamp");
        urlParams.put("sort", "timestamp+asc");

        String url = solrService.getSolrSelectURI(urlParams);

        List<String> dateRange = new ArrayList<String>();

        JSONObject jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url));
        dateRange.add((String) ((JSONObject) ((JSONArray) ((JSONObject) jsonObject.get("response")).get("docs")).get(0)).get("timestamp"));

        urlParams.put("sort", "timestamp+desc");
        jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url));
        dateRange.add((String) ((JSONObject) ((JSONArray) ((JSONObject) jsonObject.get("response")).get("docs")).get(0)).get("timestamp"));

        try {
            Long ms = (DateField.parseDate(dateRange.get(1)).getTime() - DateField.parseDate(dateRange.get(0)).getTime())/buckets;
            dateRange.add(DateUtils.getDateGapString(ms));
        } catch (ParseException e) {
            System.err.println(e.getCause());
        }

        return dateRange;
    }

    private List<String> getFieldNameSubset(Collection<String> fieldNames, boolean facetFields) {
        List<String> fieldNamesSubset = new ArrayList<String>();

        for(String fieldName : fieldNames) {
            Matcher m = fieldNamesToIgnore.matcher(fieldName);
            if ((!facetFields && fieldName.equals(solrService.getSolrSchemaHDFSKey())) || !m.matches()) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    private TreeMap<String, String> getFieldNamesAndTypesFromLuke(boolean facetFields) {
        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();
        List<String> fieldNames = new ArrayList<String>();

        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("numTerms", "0");
        urlParams.put("wt", "json");
        String url = solrService.getSolrLukeURI(urlParams);

        String response = HttpClientUtils.httpGetRequest(url);

        try {
            JSONObject json = JSONObject.fromObject(response);
            JSONObject fields = (JSONObject) json.get("fields");

            List<Object> fieldNameObjects = Arrays.asList(fields.names().toArray());
            for(Object f : fieldNameObjects) {
                fieldNames.add((String) f);
            }

            for(String field : getFieldNameSubset(fieldNames, facetFields)) {
                JSONObject fieldInfo = (JSONObject) fields.get(field);

                if (fieldInfo.containsKey("type") && !(fieldInfo.get("type") instanceof JSONNull)) {
                    namesAndTypes.put(field, (String) fieldInfo.get("type"));
                }
            }
        } catch (JSONException e) {
            System.out.println(e.getMessage());
        }
        return namesAndTypes;
    }

    private String editFilterQueryDateRange(String fq, String fieldName) {
        if (fq == null || !fq.contains(fieldName)) return fq;

        Pattern p = Pattern.compile(".*\\+" + fieldName + ":\\(([^\\)]*)\\)(\\+.*)*");
        Matcher m = p.matcher(fq);
        if (m.matches()) {
            String[] dates = m.group(1).replaceAll("^\"|\"$", "").split(" - ");
            String newDateFq = DateUtils.getSolrDateFromDateString(dates[0]) + " TO " + DateUtils.getSolrDateFromDateString(dates[1]);

            fq = fq.replace(fieldName + ":(" + m.group(1) + ")", fieldName + ":[" + newDateFq + "]");
        }
        return fq;
    }

    public JSONObject suggest(String userInput, String fieldSpecificEndpoint) {
        JSONObject ret = new JSONObject();
        ret.put("suggestions", new JSONArray());

        try {
            userInput = URIUtil.encodeQuery(userInput);
        } catch (URIException e) {
            System.out.println(e.getMessage());
        }
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("wt", "json");
        urlParams.put("q", userInput);

        String url = solrService.getSolrSuggestURI(fieldSpecificEndpoint, urlParams);
        String response = HttpClientUtils.httpGetRequest(url);

        try {
            JSONObject jsonObject = JSONObject.fromObject(response);
            if (jsonObject.has("spellcheck")) {
                JSONObject spellcheck = (JSONObject) jsonObject.get("spellcheck");
                if (spellcheck.has("suggestions")){
                    JSONArray suggestions = (JSONArray) spellcheck.get("suggestions");
                    if (suggestions.size() > 2) {
                        ret.put("suggestions", ((JSONObject) suggestions.get(1)).get("suggestion"));
                    }
                }
            }
        } catch (JSONException e) {
            System.out.println(e.getMessage());
        }

        return ret;
    }

    public void writeFacets(String coreName, TreeMap<String, String> facetFields, StringWriter writer) {
        if (facetFields == null || facetFields.size() == 0) {
            facetFields = getFieldNamesAndTypesFromLuke(true);
        }

        try {
            QueryResponse rsp = execQuery("*:*", coreName, "asc", "date", 0, 10, null, facetFields);
            writeResponse(false, rsp, 0, writer);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, int rows, String fq, TreeMap<String, String> facetFields, StringWriter writer) {

        if (facetFields == null || facetFields.size() == 0) {
            facetFields = getFieldNamesAndTypesFromLuke(true);
        }

        try {
            QueryResponse rsp = execQuery(queryString, coreName, sortType, sortOrder, start, rows, fq, facetFields);
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

    public QueryResponse execQuery(String queryString, String coreName, String sortType, String sortOrder,
                                    int start, int rows, String fq, TreeMap<String, String> facetFields) throws IOException {

        String url = solrService.getSolrServerURI();

        try {
            SolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.setStart(start);
            query.setRows(rows);

            if (facetFields != null && facetFields.size() > 0) {
                query.add("facet", "true");
                query.setFacetLimit(facetFields.size());

                for (Map.Entry field : facetFields.entrySet()) {
                    if (field.getValue().equals("date")) {
                        List<String> dateRange = getSolrIndexDateRange(coreName);
                        query.add("facet.date", (String) field.getKey());
                        query.add("facet.date.start", dateRange.get(0));
                        query.add("facet.date.end", dateRange.get(1));
                        query.add("facet.date.gap", dateRange.get(2));

                        fq = editFilterQueryDateRange(fq, (String) field.getKey());
                        if (sortType.equals("date")) {
                            query.addSortField((String) field.getKey(),
                                    sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc);
                        }
                    } else {
                        query.add("facet.field", (String) field.getKey());
                        query.add("facet.method", "enum");
                    }
                }
            }

            if (fq != null) {
                query.addFilterQuery(fq);
            }
            if (!sortType.equals("date")) {
                query.addSortField(sortType, sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc);
            }

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
            g.writeStringField("name", facetField.getName());

            g.writeArrayFieldStart("values");
            for(FacetField.Count count : facetField.getValues()) {
                if (count.getCount() > 0) {
                    g.writeString(count.getName() + " (" + count.getCount() + ")");
                }
            }
            g.writeEndArray();
            g.writeEndObject();
        }

        if (!(rsp.getFacetDates().size() == 1 && rsp.getFacetDates().get(0).getValues() == null)) {
            for(FacetField facetField : rsp.getFacetDates()) {
                g.writeStartObject();
                g.writeStringField("name", facetField.getName());

                g.writeArrayFieldStart("values");
                for(FacetField.Count count : facetField.getValues()) {
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
        g.writeEndArray();
    }
}
