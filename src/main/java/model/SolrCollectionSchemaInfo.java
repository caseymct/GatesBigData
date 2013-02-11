package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.JsonParsingUtils;
import GatesBigData.utils.SolrUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;


public class SolrCollectionSchemaInfo {
    static final String LUKE_COPYSOURCES_KEY = "copySources";
    static final String LUKE_SCHEMA_KEY      = "schema";
    static final String LUKE_FIELDS_KEY      = "fields";
    static final String LUKE_DATE_TYPE_KEY   = "date";

    private String collectionName;
    private Map<String, LukeResponse.FieldInfo> fieldInfoMap;
    private List<String> fieldNames;
    private List<String> viewFieldNames = new ArrayList<String>();
    private Map<String, Object> indexInfoMap = new HashMap<String, Object>();
    private Map<String, String> prefixFieldToCopySourceMap = new HashMap<String, String>();
    private FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();
    private NamedList<Object> indexInfo;

    private static Logger logger = Logger.getLogger(SolrCollectionSchemaInfo.class);

    public SolrCollectionSchemaInfo(String collectionName, SolrServer server) {
        this.collectionName = collectionName;
        init(server);
    }

    private void init(SolrServer server) {
        LukeRequest luke = new LukeRequest();
        LukeResponse rsp;

        try {
            rsp = luke.process(server);

            initFieldInfoVars(rsp);
            initIndexInfoVars(rsp);
            initFacetFieldEntryList();

            luke.setShowSchema(true);
            rsp = luke.process(server);
            initPrefixFieldToCopySourceMap(rsp);

        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    private void initFieldInfoVars(LukeResponse rsp) {
        fieldInfoMap = rsp.getFieldInfo();
        fieldNames = new ArrayList<String>(fieldInfoMap.keySet());
        Collections.sort(fieldNames);

        for(String field : fieldNames) {
            Matcher m = Constants.VIEW_FIELD_NAMES_PATTERN.matcher(field);
            if (!m.matches()) {
                viewFieldNames.add(field);
            }
        }
    }

    private void initIndexInfoVars(LukeResponse rsp) {
        indexInfo = rsp.getIndexInfo();
        for(Map.Entry<String, Object> entry : indexInfo) {
            indexInfoMap.put(entry.getKey(), entry.getValue());
        }
    }

    private void initPrefixFieldToCopySourceMap(LukeResponse rsp) {
        NamedList schema = (NamedList) rsp.getResponse().get(LUKE_SCHEMA_KEY);
        NamedList fields = (NamedList) schema.get(LUKE_FIELDS_KEY);

        for(int i = 0; i < fields.size(); i++) {
            String fieldName = fields.getName(i);
            if (!fieldName.startsWith(Constants.SOLR_CONTENT_FIELD_NAME)
                    && fieldName.endsWith(SolrUtils.SOLR_SCHEMA_PREFIXFIELD_ENDSWITH)) {
                NamedList properties = (NamedList) fields.getVal(i);
                List<String> copyFields = (ArrayList<String>) properties.get(LUKE_COPYSOURCES_KEY);
                if (copyFields.size() > 0) {
                    Matcher m = Constants.COPYSOURCE_PATTERN.matcher(copyFields.get(0));
                    if (m.matches()) {
                        prefixFieldToCopySourceMap.put(fieldName, m.group(1));
                    }
                }
            }
        }
    }


    private void initFacetFieldEntryList() {
        for(Map.Entry<String, LukeResponse.FieldInfo> entry : fieldInfoMap.entrySet()) {
            String fieldName = entry.getKey();
            LukeResponse.FieldInfo fieldInfo = entry.getValue();

            if (SolrUtils.validFieldName(fieldName, true)) {
                facetFieldEntryList.add(fieldName, fieldInfo.getType(), fieldInfo.getSchema());
            }
        }
    }

    public JSONObject getIndexInfoAsJSON() {
        return JsonParsingUtils.constructJSONObjectFromNamedList(indexInfo);
    }
    public boolean isFieldMultiValued(String fieldName) {
        LukeResponse.FieldInfo fieldInfo = fieldInfoMap.get(fieldName);
        return fieldInfo != null && fieldInfo.getSchema().charAt(3) == 77;
    }

    public boolean fieldExists(String fieldName) {
        return fieldInfoMap.containsKey(fieldName);
    }

    public List<String> getFieldNamesByType(String type) {
        List<String> fieldsByType = new ArrayList<String>();

        for(Map.Entry<String, LukeResponse.FieldInfo> entry : fieldInfoMap.entrySet()) {
            String fieldName = entry.getKey();
            LukeResponse.FieldInfo fieldInfo = entry.getValue();

            if (fieldInfo.getType().equals(type)) {
                fieldsByType.add(fieldName);
            }
        }
        Collections.sort(fieldsByType);
        return fieldsByType;
    }

    public List<String> getDateFields() {
        return getFieldNamesByType(LUKE_DATE_TYPE_KEY);
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<String> getViewFieldNames() {
        return viewFieldNames;
    }

    public String getViewFieldNamesString() {
        return StringUtils.join(viewFieldNames, Constants.DEFAULT_DELIMETER);
    }

    public Map<String, String> getPrefixFieldMap() {
        return prefixFieldToCopySourceMap;
    }

    public List<String> getFacetFieldNames() {
        return facetFieldEntryList.getNames();
    }

    public FacetFieldEntryList getFacetFieldEntryList() {
        return facetFieldEntryList;
    }

    public List<String> getFieldNamesSubset(List<String> indices, boolean include) {
        List<String> subList = new ArrayList<String>();

        for(int i = 0; i < viewFieldNames.size(); i++) {
            if ((include && indices.contains(i + "")) || (!include && !indices.contains(i + ""))) {
                subList.add(viewFieldNames.get(i));
            }
        }
        return subList;
    }
}
