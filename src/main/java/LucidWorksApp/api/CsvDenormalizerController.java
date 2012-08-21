package LucidWorksApp.api;

import LucidWorksApp.search.SolrUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;


@Controller
@RequestMapping("/csvdenormalize")
public class CsvDenormalizerController extends APIController {

    private static final String DELIM = ";";

    private JSONObject updateJsonObject(String[] mapKeys, HashMap<String, String> map,
                                    List<String> keys, List<String> values, String jsonObjectTitle,
                                    JSONObject jsonObject) {
        String[] mapString = new String[mapKeys.length];
        for(int i = 0; i < mapKeys.length; i++) {
            mapString[i] = values.get(keys.indexOf(mapKeys[i]));
        }
        String mapKey = StringUtils.join(mapString, DELIM);

        if (map.containsKey(mapKey)) {
            jsonObject.put(jsonObjectTitle, JSONObject.fromObject(map.get(mapKey)));
        }
        return jsonObject;
    }

    private void writeJsonObjectToFile(String fileName, JSONObject jsonObject) {
        try{
            FileWriter fstream = new FileWriter("c:\\Users\\cm0607\\projects\\omega\\outFiles\\" + fileName);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(jsonObject.toString(5));
            out.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    private HashMap<String, String> getCSVMap(String[] mainKeys, String fileName) {
        HashMap<String, String> csvMap = new HashMap<String, String>();
        List<String> keys = new ArrayList<String>();

        System.out.println(fileName);
        try {
            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine = br.readLine();

            keys = new ArrayList<String>(Arrays.asList(strLine.replaceAll("^\\\"|\\\"$", "").split("\",\"")));
            int[] indices = new int[mainKeys.length];
            for(int i = 0; i < mainKeys.length; i++) {
                indices[i] = keys.indexOf(mainKeys[i]);
            }

            while ((strLine = br.readLine()) != null)   {
                String[] csvmapKey = new String[mainKeys.length];
                String[] csvmapSplit = strLine.split("\",\"");
                csvmapSplit[0] = csvmapSplit[0].replace("\"", "");
                csvmapSplit[csvmapSplit.length-1] = csvmapSplit[csvmapSplit.length-1].replace("\"", "");

                for(int i = 0; i < indices.length; i++) {
                    csvmapKey[i] = csvmapSplit[indices[i]];
                }

                JSONObject jsonObject = new JSONObject();
                for(int i = 0; i < keys.size(); i++) {
                    jsonObject.put(keys.get(i), csvmapSplit[i].replaceAll("\"", ""));
                }
                csvMap.put(StringUtils.join(csvmapKey, DELIM), jsonObject.toString());
            }

            in.close();
        } catch (Exception e) {
            System.out.println(e);
        }

        System.out.println("about to return csvmap");
        return csvMap;
    }

    @RequestMapping(value="/index", method = RequestMethod.GET)
    public ResponseEntity<String> denormalize() throws IOException {

        String[] supplierKeys = new String[] {"SupplierId", "SupplierLocationId"};
        HashMap<String, String> supplierMap = getCSVMap(supplierKeys, "c:\\Users\\cm0607\\projects\\omega\\Supplier.csv");
        String[] costCenterKeys = new String[] {"CostCenterId", "CompanyCode"};
        HashMap<String, String> costCenterMap = getCSVMap(costCenterKeys, "c:\\Users\\cm0607\\projects\\omega\\CostCenter.csv");
        String[] accountKeys = new String[] {"AccountId", "CompanyCode"};
        HashMap<String, String> accountMap = getCSVMap(accountKeys, "c:\\Users\\cm0607\\projects\\omega\\Account.csv");
        String[] companySiteKeys = new String[] {"SiteId"};
        HashMap<String, String> companySiteMap = getCSVMap(companySiteKeys, "c:\\Users\\cm0607\\projects\\omega\\CompanySite.csv");
        String[] erpCommodityKeys = new String[] {"CommodityId"};
        HashMap<String, String> erpCommodityMap = getCSVMap(erpCommodityKeys, "c:\\Users\\cm0607\\projects\\omega\\ERPCommodity.csv");
        String[] flexDimension1Keys = new String[] {"FieldId"};
        HashMap<String, String> flexDimension1Map = getCSVMap(flexDimension1Keys, "c:\\Users\\cm0607\\projects\\omega\\FlexDimension1.csv");
        String[] partKeys = new String[] {"PartNumber", "RevisionNumber"};
        HashMap<String, String> partMap = getCSVMap(partKeys, "c:\\Users\\cm0607\\projects\\omega\\Part.csv");
        String[] PO2Keys = new String[] {"POId", "ExtraPOKey", "POLineNumber", "ExtraPOLineKey", "SplitAccountingNumber"};
        HashMap<String, String> po2Map = getCSVMap(PO2Keys, "c:\\Users\\cm0607\\projects\\omega\\PO2.csv");
        String[] userKeys = new String[] {"UserId"};
        HashMap<String, String> userMap = getCSVMap(userKeys, "c:\\Users\\cm0607\\projects\\omega\\User.csv");

        try {
            FileInputStream fstream = new FileInputStream("c:\\Users\\cm0607\\projects\\omega\\Ariba_Invoice2\\Invoice2.csv");

            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine = br.readLine();

            //List<String> keys = new ArrayList<String>(Arrays.asList(strLine.split("\",\"")));
            List<String> keys = new ArrayList<String>(Arrays.asList(strLine.replaceAll("^\\\"|\\\"$", "").split("\",\"")));

            int count = 0;
            while ((strLine = br.readLine()) != null)   {

                List<String> values = new ArrayList<String>(Arrays.asList(strLine.split("\",\"")));
                values.set(0, values.get(0).replace("\"", ""));
                values.set(values.size()-1, values.get(values.size()-1).replace("\"", ""));

                JSONObject jsonObject = new JSONObject();

                while (values.size() < keys.size()) {
                    strLine = strLine + "\n" + br.readLine();
                    values = new ArrayList<String>(Arrays.asList(strLine.split("\",\"")));
                }

                for(int i = 0; i < values.size(); i++) {
                    jsonObject.put(keys.get(i), values.get(i));
                }

                jsonObject = updateJsonObject(supplierKeys, supplierMap, keys, values, "Supplier", jsonObject);
                jsonObject = updateJsonObject(new String[] {"CostCenterId", "CostCenterCompanyCode"}, costCenterMap, keys, values, "CostCenter", jsonObject);
                jsonObject = updateJsonObject(new String[] {"AccountId", "AccountCompanyCode"}, accountMap, keys, values, "Account", jsonObject);
                jsonObject = updateJsonObject(new String[] {"ERPCommodityId"}, erpCommodityMap, keys, values, "ERPCommodity", jsonObject);
                jsonObject = updateJsonObject(new String[] {"CompanySiteId"}, companySiteMap, keys, values, "CompanySite", jsonObject);
                jsonObject = updateJsonObject(new String[] {"FlexFieldId1"}, flexDimension1Map, keys, values, "FlexDimension1", jsonObject);
                jsonObject = updateJsonObject(new String[] {"PartNumber", "PartRevisionNumber"}, partMap, keys, values, "Part", jsonObject);
                jsonObject = updateJsonObject(PO2Keys, po2Map, keys, values, "PO2", jsonObject);
                jsonObject = updateJsonObject(new String[] {"RequesterId"}, userMap, keys, values, "User", jsonObject);

                writeJsonObjectToFile("out" + (++count), jsonObject);
            }

            in.close();
        }catch (Exception e){//Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>("", httpHeaders, OK);
    }
}
