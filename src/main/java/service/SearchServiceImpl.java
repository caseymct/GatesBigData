package service;

import LucidWorksApp.utils.DateUtils;
import LucidWorksApp.utils.HttpClientUtils;
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
    private String SOLR_DEFAULT_SORT_FIELD = "score";
    private String SOLR_DEFAULT_QUERY = "*:*";
    private int SOLR_DEFAULT_START = 0;
    private int SOLR_DEFAULT_ROWS = 10;
    private SolrQuery.ORDER SOLR_DEFAULT_SORT_ORDER = SolrQuery.ORDER.asc;

    private SolrService solrService;
    private CoreService coreService;

    @Autowired
    public void setServices(SolrService solrService, CoreService coreService) {
        this.solrService = solrService;
        this.coreService = coreService;
        fieldNamesToIgnore = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
                solrService.getSolrSchemaHDFSKey() +
                "|boost|digest|host|segment|tstamp).*|.*id");
    }

    private SolrQuery.ORDER getSortOrder(String sortOrder) {
        return sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc;
    }

    public List<String> getSolrIndexDateRange(String coreName) {

        int buckets = 10;
        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("q", "timestamp:*");
        urlParams.put("rows", "1");
        urlParams.put("wt", "json");
        urlParams.put("fl", "timestamp");
        urlParams.put("sort", "timestamp+asc");

        String url = solrService.getSolrSelectURI(coreName, urlParams);

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

    private TreeMap<String, String> getFieldNamesAndTypesFromLuke(String coreName, boolean facetFields) {
        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();
        List<String> fieldNames = new ArrayList<String>();

        String response = solrService.getSolrLukeData(coreName);

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

    private String getSolrSuggestion(String coreName, String userInput, String prefixField, String fullField) {

        HashMap<String,String> urlParams = new HashMap<String, String>();
        urlParams.put("wt", "json");
        urlParams.put("q", prefixField + ":" + userInput + "*");
        urlParams.put("fl", fullField);
        urlParams.put("group.field", fullField);
        urlParams.put("group", "true");
        String url = solrService.getSolrSelectURI(coreName, urlParams);
        //http://denlx006.dn.gates.com:8983/solr/select?q=UserNamePrefix:c*&fl=User.UserName&group=true&group.field=User.UserName

        return HttpClientUtils.httpGetRequest(url);
    }

    public JSONObject suggest(String coreName, String userInput, String fieldSpecificEndpoint) {
        try {
            userInput = URIUtil.encodeQuery(userInput);
        } catch (URIException e) {
            System.out.println(e.getMessage());
        }

        JSONObject ret = new JSONObject();
        JSONArray suggestions = new JSONArray();

        String prefixField = solrService.getSolrSchemaFieldName(fieldSpecificEndpoint, true);
        String fullField = solrService.getSolrSchemaFieldName(fieldSpecificEndpoint, false);
        String response = getSolrSuggestion(coreName, userInput, prefixField, fullField);

        try {
            JSONObject jsonObject = JSONObject.fromObject(response);
            if (jsonObject.has("grouped")) {
                JSONObject userName = jsonObject.getJSONObject("grouped").getJSONObject(fullField);
                if (userName.has("groups")){
                    JSONArray groups = userName.getJSONArray("groups");
                    if (groups.size() > 0) {
                        for(int i = 0; i < groups.size(); i++) {
                            JSONObject g = ((JSONObject) groups.get(i)).getJSONObject("doclist");
                            String suggest = (String) ((JSONObject) g.getJSONArray("docs").get(0)).get(fullField);
                            suggestions.add(suggest + " (" + Integer.toString(g.getInt("numFound")) + ")");
                        }
                    }
                }
            }
        } catch (JSONException e) {
            System.out.println(e.getMessage());
        }

        ret.put("suggestions", suggestions);
        return ret;
    }

    public void writeFacets(String coreName, TreeMap<String, String> facetFields, StringWriter writer) {
        if (facetFields == null || facetFields.size() == 0) {
            facetFields = getFieldNamesAndTypesFromLuke(coreName, true);
        }

        try {
            QueryResponse rsp = execQuery(SOLR_DEFAULT_QUERY, coreName, SOLR_DEFAULT_SORT_FIELD, SOLR_DEFAULT_SORT_ORDER,
                    SOLR_DEFAULT_START, SOLR_DEFAULT_ROWS, null, facetFields);
            writeResponse(false, rsp, 0, writer);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, int rows, String fq, TreeMap<String, String> facetFields, StringWriter writer) {

        if (facetFields == null || facetFields.size() == 0) {
            facetFields = getFieldNamesAndTypesFromLuke(coreName, true);
        }

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
                                    int start, int rows, String fq, TreeMap<String, String> facetFields) {

        String url = solrService.getSolrServerURI(coreName);

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

                for (Map.Entry field : facetFields.entrySet()) {
                    if (field.getValue().equals("date")) {
                        List<String> dateRange = getSolrIndexDateRange(coreName);
                        query.add("facet.date", (String) field.getKey());
                        query.add("facet.date.start", dateRange.get(0));
                        query.add("facet.date.end", dateRange.get(1));
                        query.add("facet.date.gap", dateRange.get(2));

                        fq = editFilterQueryDateRange(fq, (String) field.getKey());
                    } else {
                        query.add("facet.field", (String) field.getKey());
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
