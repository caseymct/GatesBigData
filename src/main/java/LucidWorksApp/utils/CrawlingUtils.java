package LucidWorksApp.utils;

import java.util.List;

public class CrawlingUtils extends Utils {

    public static String startCrawl(String url) {
        return HttpClientUtils.httpPutRequest(url);
    }

    public static String stopCrawl(String url) {
        return HttpClientUtils.httpDeleteRequest(url);
    }

    public static boolean areAllCrawling(String collectionName) {
        for (String crawlerStatus : getCrawlStatuses(collectionName, "all")) {
            if (!crawlerStatus.equals("RUNNING")) {
                return false;
            }
        }
        return true;
    }

    public static boolean areAnyCrawling(String collectionName) {
        for (String crawlerStatus : getCrawlStatuses(collectionName, "all")) {
            if (crawlerStatus.equals("RUNNING")) {
                return true;
            }
        }
        return false;
    }

    public static String getCrawlerStatus(String collectionName) {
        String status = "IDLE";
        for (String crawlerStatus : getCrawlStatuses(collectionName, "all")) {
            if (crawlerStatus.equals("RUNNING")) {
                return "RUNNING";
            }
            if (crawlerStatus.equals("STOPPED")) {
                status = "STOPPED";
            }
        }
        return status;
    }

    public static boolean isCrawling(String collectionName, String id) {
        List<String> crawlStatus = getCrawlStatuses(collectionName, id);
        return crawlStatus.get(0).equals("RUNNING");
    }

    public static List<String> getCrawlStatuses(String collectionName, String dataSourcesToCheck) {
        /* dataSourcesToCheck should be 'all' or an id */

        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + "/datasources/" +
                    dataSourcesToCheck + "/job";
        String crawlerDetails = HttpClientUtils.httpGetRequest(url);

        return convertObjectListToStringList(JsonParsingUtils.getPropertiesFromDataSourceJson("crawl_state", crawlerDetails));
    }

    public static String getCrawlerIdByType(String collectionName, String crawlerType) {
        String url = SERVER + COLLECTIONS_ENDPOINT + "/" + collectionName + DATASOURCES_ENDPOINT;

        String dataSrcFullJson = HttpClientUtils.httpGetRequest(url);
        Object id = JsonParsingUtils.getContingentPropertyFromDataSourceJson("type", crawlerType, "id", dataSrcFullJson);

        if (id instanceof Integer) {
            return id.toString();
        }
        return "Collection " + collectionName + " does not have a crawler of type " + crawlerType;
    }
}

