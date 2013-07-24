package GatesBigData.utils;

import GatesBigData.constants.Constants;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

import static GatesBigData.constants.Constants.*;
import static GatesBigData.utils.Utils.nullOrEmpty;

public class URLUtils {

    private static final Logger logger = Logger.getLogger(URLUtils.class);

    public static String encodeQuery(String s) {
        try {
            s = URIUtil.encodeQuery(s);
        } catch (URIException e) {
            logger.error(e.getMessage());
        }
        return s;
    }

    public static String decodeUrl(String url) {
        try {
            url = URLDecoder.decode(url, UTF8);
        } catch (UnsupportedEncodingException e) {
            logger.error("Could not decode URL: " + e.getMessage());
        }
        return url;
    }

    public static String encodeUrlComponent(String s) {
        try {
            return URLEncoder.encode(s, Constants.UTF8);
        } catch (UnsupportedEncodingException e) {
            logger.error("Could not encode string: " + e.getMessage());
        }
        return s;
    }

    private static List<String> getParamListFromHashMap(HashMap<String, String> params) {
        List<String> paramList = new ArrayList<String>();
        for(Map.Entry<String,String> entry : params.entrySet()) {
            paramList.add(encodeUrlComponent(entry.getKey()) + "=" + encodeUrlComponent(entry.getValue()));
        }
        return paramList;
    }

    public static String constructUrlParams(HashMap<String, String> params) {
        return params == null ? "" : "?" + StringUtils.join(getParamListFromHashMap(params), "&");
    }

    public static String constructAddress(String uri, HashMap<String, String> params) {
        return uri + constructUrlParams(params);
    }

    public static String constructAddress(String protocol, String server, int port) {
        return protocol + server + ":" + port;
    }

    public static String constructAddress(String protocol, String server, int port, String endpoint) {
        return constructAddress(protocol, server, port, Arrays.asList(endpoint));
    }

    public static String constructAddress(String protocol, String server, int port, List<String> endpoints) {
        String uri = constructAddress(protocol, server, port);
        return constructAddress(uri, endpoints);
    }

    public static String constructAddress(String uri, String endpoint) {
        return constructAddress(uri, Arrays.asList(endpoint));
    }

    public static String constructAddress(String uri, List<String> endpoints) {
        for(String endpoint : endpoints) {
            if (!nullOrEmpty(endpoint)) {
                uri += "/" + endpoint.replaceAll("^/|/$", "");
            }
        }
        return uri;
    }

    public static String constructAddress(String uri, List<String> endpoints, HashMap<String, String> params) {
        return constructAddress(uri, endpoints) + constructUrlParams(params);
    }
}
