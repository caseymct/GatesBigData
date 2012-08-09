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

import java.net.MalformedURLException;


public class SolrUtils extends Utils {

    private static String SOLR_ENDPOINT = "/solr";
    private static String LUCID_ENDPOINT = "/lucid";
    private static String UPDATECSV_ENDPOINT = "/update/csv";

    public static String lucidEndpointSearch(String collectionName, String queryParams) {
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName + LUCID_ENDPOINT;
        queryParams += "&wt=json&role=DEFAULT";

        System.out.println("Full request " + url + queryParams);
        return HttpClientUtils.httpGetRequest(url + queryParams);
    }

    public static void solrSearch(String collectionName, String queryString) {
        String url = SERVER + SOLR_ENDPOINT + "/" + collectionName;

        try {
            //SolrServer server = new HttpSolrServer(url);
            SolrServer server = new CommonsHttpSolrServer(url);
            SolrQuery query = new SolrQuery();

            query.setQuery(queryString);
            query.addSortField("relevance", SolrQuery.ORDER.desc);
            QueryResponse rsp = server.query( query );
            SolrDocumentList docs = rsp.getResults();
            for (SolrDocument doc : docs) {
                System.out.println((String)doc.getFieldValue("id")+": ");
                System.out.println(doc.getFieldValue("title"));
                System.out.println(doc.getFieldValue("body"));

            }
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
