package LucidWorksApp.utils;

import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
//import org.apache.http.client.HttpClient;
import org.apache.commons.httpclient.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.springframework.web.client.HttpClientErrorException;

import java.io.*;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class HttpClientUtils {

    private static String getResponse(HttpEntity entity) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;

        if (entity != null) {
            InputStream inputStream = entity.getContent();
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

            while((line = in.readLine()) != null) {
                sb.append(line);
            }
            inputStream.close();
        }

        return sb.toString();
    }

    public static String httpJsonPostRequest(String url, String json) {

        DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost postRequest = new HttpPost(url);
            StringEntity input = new StringEntity(json);
            input.setContentType(Constants.JSON_CONTENT_TYPE);
            postRequest.setEntity(input);

            HttpResponse response = httpClient.execute(postRequest);
            HttpEntity responseEntity = response.getEntity();

            return getResponse(responseEntity);

        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (org.apache.http.HttpException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }

    public static String httpBinaryDataPostRequest(String url, String fileName) {

        DefaultHttpClient httpclient = new DefaultHttpClient();

        try {
            HttpPost postRequest = new HttpPost(url);
            //StringEntity input = new StringEntity(message,
            //        ContentType.create("text/plain", "UTF-8"));

            File file = new File(fileName);
            if (!file.exists()) {
                return "<title>File not found: " + fileName + "</title>";
            }

            InputStreamEntity reqEntity = new InputStreamEntity(new FileInputStream(file), -1);
            reqEntity.setContentType("binary/octet-stream");
            reqEntity.setChunked(true);
            // FileEntity entity = new FileEntity(file, "binary/octet-stream");

            postRequest.setEntity(reqEntity);

            System.out.println("executing request " + postRequest.getRequestLine());
            HttpResponse response = httpclient.execute(postRequest);
            HttpEntity responseEntity = response.getEntity();

            System.out.println("----------------------------------------");
            System.out.println(response.getStatusLine());

            return getResponse(responseEntity);

        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (org.apache.http.HttpException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }

    public static String httpPostRequest(String url, HashMap<String, String> parameters) {
        HttpClient httpclient = new HttpClient();

        PostMethod method = new PostMethod(url);

        for(Map.Entry<String, String> entry : parameters.entrySet()) {
            method.addParameter(entry.getKey(), entry.getValue());
        }

        try {
            httpclient.executeMethod(method);
            String response = method.getResponseBodyAsString();
            System.out.println(response);
            method.releaseConnection( );
            return response;
            //return getResponse(response.getEntity());

        } catch (HttpClientErrorException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }

    public static String httpGetRequest(String url) {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        try {
            HttpGet httpget = new HttpGet(url);
            HttpResponse response = httpclient.execute(httpget);

            return getResponse(response.getEntity());

        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (org.apache.http.HttpException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }

    public static String httpPutRequest(String url) {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        try {
            HttpPut httpPut = new HttpPut(url);
            HttpResponse response = httpclient.execute(httpPut);

            return getResponse(response.getEntity());

        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (org.apache.http.HttpException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }

    public static String httpJsonPutRequest(String url, String json) {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        try {
            HttpPut httpPut = new HttpPut(url);

            StringEntity input = new StringEntity(json);
            input.setContentType(Constants.JSON_CONTENT_TYPE);
            httpPut.setEntity(input);

            HttpResponse response = httpclient.execute(httpPut);

            return getResponse(response.getEntity());

        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (org.apache.http.HttpException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }

    public static String httpDeleteRequest(String url) {
        DefaultHttpClient httpclient = new DefaultHttpClient();

        try {
            HttpDelete httpDelete = new HttpDelete(url);
            HttpResponse response = httpclient.execute(httpDelete);

            return getResponse(response.getEntity());

        } catch (URISyntaxException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (org.apache.http.HttpException e) {
            System.out.println(e.getMessage());
        }

        return "";
    }
}
