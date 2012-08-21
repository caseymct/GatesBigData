package service;

import LucidWorksApp.utils.DateUtils;
import LucidWorksApp.utils.FieldUtils;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
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

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SolrServiceImpl implements SolrService {

    private static String SOLR_ENDPOINT = "/solr";
    private static String UPDATECSV_ENDPOINT = "/update/csv";
    private static String LUKE_ENDPOINT = "/admin/luke";
    private static Pattern fieldNamesToIgnore = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all).*");
    private static Pattern fieldNamesToIgnoreIncIds = Pattern.compile("^(attr|_).*|.*(version|batch|body|text_all|Id).*");
    private String SERVER;

    public SolrServiceImpl() {
        this.SERVER = Utils.getServer();
    }

    public List<String> getSolrIndexDateRange(String collectionName) {
        int buckets = 10;
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + "/select";
        String urlParams = "?q=*:*&rows=1&wt=json&fl=timestamp&sort=timestamp";

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

    private List<String> getFieldNameSubset(Collection<String> fieldNames, boolean ignoreIds) {
        List<String> fieldNamesSubset = new ArrayList<String>();

        for(String fieldName : fieldNames) {
            Matcher m = ignoreIds ? fieldNamesToIgnoreIncIds.matcher(fieldName) : fieldNamesToIgnore.matcher(fieldName);
            if (!m.matches()) {
                fieldNamesSubset.add(fieldName);
            }
        }
        Collections.sort(fieldNamesSubset);
        return fieldNamesSubset;
    }

    /* Return the field name and the type
   * //http://denlx006.dn.gates.com:8888/solr/invoice2test/admin/luke?numTerms=0&wt=json
   * */
    private TreeMap<String, String> retrieveFields(String collectionName) {
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + LUKE_ENDPOINT;
        String urlParams = "?numTerms=0&wt=json";

        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();
        JSONObject json = JSONObject.fromObject(HttpClientUtils.httpGetRequest(url+urlParams));

        for(String field : getFieldNameSubset(((JSONObject) json.get("fields")).names(), false)) {
            String type = (String) ((JSONObject)((JSONObject) json.get("fields")).get((String)field)).get("type");
            namesAndTypes.put(field, type);
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

    public void solrSearch(String collectionName, String queryString, String sortType, String sortOrder,
                           int start, String fq, JsonGenerator g) throws IOException {

        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName;
        TreeMap<String, String> fields = retrieveFields(collectionName);

        try {
            //SolrServer server = new HttpSolrServer(url);
            SolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.setStart(start);

            query.add("facet", "true");
            for (Map.Entry field : fields.entrySet()) {
                if (field.getValue().equals("date")) {
                    List<String> dateRange = getSolrIndexDateRange(collectionName);
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
            SolrDocumentList docs = rsp.getResults();

            g.writeNumberField("start", start);
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

            g.writeEndArray();
        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        }
    }

    public String importCsvFileOnServerToSolr(String collectionName, String fileName) {
        /*
        String url = SERVER + SOLR_ENDPOINT + UPDATECSV_ENDPOINT;
        String urlParams = "?stream.file=" + fileName + "&stream.contentType=text/plain;charset=utf-8";
        straight solr
        */
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + UPDATECSV_ENDPOINT;
        String urlParams = "?commit=true&f.categories.split=true&stream.file=" +
                fileName + "&stream.contentType=text/csv";

        String response = HttpClientUtils.httpGetRequest(url + urlParams);
        if (!response.contains("Errors")) {
            FieldUtils.updateCSVFilesUploadedField(collectionName, fileName, true);
        }
        return response;
    }

    public String importCsvFileOnLocalSystemToSolr(String collectionName, String fileName) {
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + UPDATECSV_ENDPOINT;
        String urlParams = "?commit=true&f.categories.split=true";

        //curl http://localhost:8983/solr/update/csv --data-binary @books.csv -H 'Content-type:text/plain; charset=utf-8'

        // SolrServer server = new CommonsHttpSolrServer(SERVER + SOLR_ENDPOINT);
        // ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/csv");
        // req.addFile(new File(filename));
        // req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
        // NamedList result = server.request(req);
        // System.out.println("Result: " + result);

        String response = HttpClientUtils.httpBinaryDataPostRequest(url + urlParams, fileName);
        if (!response.contains("Errors")) {
            FieldUtils.updateCSVFilesUploadedField(collectionName, fileName, false);
        }
        return response;
    }
}
}
