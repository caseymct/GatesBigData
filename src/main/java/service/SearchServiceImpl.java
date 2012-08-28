package service;

import LucidWorksApp.utils.DateUtils;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;
import org.apache.avro.generic.GenericData;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.schema.DateField;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SearchServiceImpl implements SearchService {

    private static Pattern fieldNamesToIgnore = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|" +
                                                                 Utils.getSolrSchemaHdfskey() + ").*");

    public List<String> getSolrIndexDateRange(String collectionName) {

        int buckets = 10;
        String url = Utils.getServer() + Utils.getSolrEndpoint() + "/select";
        String urlParams = "?q=timestamp:*&rows=1&wt=json&fl=timestamp&sort=timestamp";

        List<String> dateRange = new ArrayList<String>();

        JSONObject jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url + urlParams + "+asc"));
        dateRange.add((String) ((JSONObject) ((JSONArray) ((JSONObject) jsonObject.get("response")).get("docs")).get(0)).get("timestamp"));

        jsonObject = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url + urlParams + "+desc"));
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
            if ((!facetFields && fieldName.equals(Utils.getSolrSchemaHdfskey())) || !m.matches()) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    private TreeMap<String, String> getFieldNamesAndTypesFromLuke(boolean facetFields) {
        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();
        List<String> fieldNames = new ArrayList<String>();

        String url = Utils.getServer() + Utils.getSolrEndpoint() + Utils.getLukeEndpoint();
        String urlParams = "?numTerms=0&wt=json";
        String response = HttpClientUtils.httpGetRequest(url+urlParams);

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
            String[] dates = m.group(1).split(" - ");
            String newDateFq = DateUtils.getSolrDateFromDateString(dates[0]) + " TO " + DateUtils.getSolrDateFromDateString(dates[1]);

            fq = fq.replace(fieldName + ":(" + dates[0] + " - " + dates[1] + ")", fieldName + ":[" + newDateFq + "]");
        }
        return fq;
    }

    public void solrSearch(String queryString, String coreName, String sortType, String sortOrder,
                           int start, String fq, TreeMap<String, String> facetFields, JsonGenerator g) throws IOException {

        if (facetFields == null || facetFields.size() == 0) {
            facetFields = getFieldNamesAndTypesFromLuke(true);
        }

        String url = Utils.getServer() + Utils.getSolrEndpoint();
        g.writeNumberField("start", start);

        try {
            //SolrServer server = new HttpSolrServer(url);
            SolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.setStart(start);

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

            if (fq != null) {
                query.addFilterQuery(fq);
            }
            if (!sortType.equals("date")) {
                query.addSortField(sortType, sortOrder.equals("asc") ? SolrQuery.ORDER.asc : SolrQuery.ORDER.desc);
            }
            QueryResponse rsp = server.query(query);
            writeSearchResponse(g, rsp);

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        }
    }

    private void writeSearchResponse(JsonGenerator g, QueryResponse rsp) throws IOException {
        SolrDocumentList docs = rsp.getResults();

        g.writeNumberField("numFound", docs.getNumFound());
        g.writeArrayFieldStart("docs");

        for (SolrDocument doc : docs) {
            g.writeStartObject();
            for(String fieldName : getFieldNameSubset(doc.getFieldNames(), false)) {
                Utils.writeValueByType(fieldName, doc.getFieldValue(fieldName), g);
            }
            g.writeEndObject();
        }
        g.writeEndArray();

        g.writeArrayFieldStart("facets");
        for(FacetField facetField : rsp.getFacetFields()) {
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
