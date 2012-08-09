package LucidWorksApp.search;

import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SolrUtils extends Utils {

    private static String SOLR_ENDPOINT = "/solr";
    private static String LUCID_ENDPOINT = "/lucid";
    private static String UPDATECSV_ENDPOINT = "/update/csv";
    private static Pattern fieldNamesToIgnore = Pattern.compile("^(attr|_).*|.*(version|batch|body).*");

    public static String lucidEndpointSearch(String collectionName, String queryParams) {
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + LUCID_ENDPOINT;
        queryParams += "&wt=json&role=DEFAULT";

        System.out.println("Full request " + url + queryParams);
        return HttpClientUtils.httpGetRequest(url + queryParams);
    }

    private static List<String> getFieldNameSubset(Collection<String> fieldNames) {
        List<String> fieldNamesSubset = new ArrayList<String>();


        for(String fieldName : fieldNames) {
            Matcher m = fieldNamesToIgnore.matcher(fieldName);
            if (!m.matches()) {
                fieldNamesSubset.add(fieldName);
            }
        }
        return fieldNamesSubset;
    }

    public static void solrSearch(String collectionName, String queryString, int start, JsonGenerator g) throws IOException {
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName;

        try {
            //SolrServer server = new HttpSolrServer(url);
            SolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.setStart(start);
            //query.addSortField("relevance", SolrQuery.ORDER.desc);
            QueryResponse rsp = server.query(query);
            SolrDocumentList docs = rsp.getResults();

            g.writeNumberField("start", start);
            g.writeNumberField("numFound", docs.getNumFound());
            g.writeArrayFieldStart("docs");

            for (SolrDocument doc : docs) {
                g.writeStartObject();
                for(String fieldName : getFieldNameSubset(doc.getFieldNames())) {
                    Utils.writeValueByType(fieldName, doc.getFieldValue(fieldName), g);
                }
                g.writeEndObject();
            }
            g.writeEndArray();

        } catch (MalformedURLException e) {
            System.out.println(e.getMessage());
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        }
    }
    //http://127.0.0.1:8888/solr/epic/select/?q=disaster&wt=json
    //http://127.0.0.1:8888/solr/epic/select/?q=twitter&wt=json&facet=on&facet.field=userScreenName&hl=true&Which

    public static String importLocalCsvToSolr(String collectionName, String fileName) {
        /*
        String url = SERVER + SOLR_ENDPOINT + UPDATECSV_ENDPOINT;
        String urlParams = "?stream.file=" + fileName + "&stream.contentType=text/plain;charset=utf-8";
        straight solr
        */

        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + UPDATECSV_ENDPOINT;
        String urlParams = "?commit=true&f.categories.split=true";

        return HttpClientUtils.httpGetRequest(url + urlParams);
    }

    public static String importRemoteCsvToSolr(String collectionName, String fileName) {
        //String url = SERVER + SOLR_ENDPOINT + UPDATECSV_ENDPOINT;
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + UPDATECSV_ENDPOINT;
        String urlParams = "?commit=true&f.categories.split=true";

        //curl http://localhost:8983/solr/update/csv --data-binary @books.csv -H 'Content-type:text/plain; charset=utf-8'


        return HttpClientUtils.httpBinaryDataPostRequest(url + urlParams, fileName);
    }
}
