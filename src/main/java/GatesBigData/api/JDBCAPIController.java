package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.JDBC;
import GatesBigData.constants.solr.FieldNames;
import GatesBigData.constants.solr.Operations;
import GatesBigData.constants.solr.Response;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.Utils;
import model.JDBCData;
import model.SolrCollectionSchemaInfo;
import model.SolrSchemaInfo;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.CoreService;
import service.HDFSService;
import service.JDBCService;
import service.SearchService;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/jdbc")
public class JDBCAPIController extends APIController {

    private static final Logger logger = Logger.getLogger(JDBCAPIController.class);

    private JDBCService jdbcService;
    private CoreService coreService;
    private SearchService searchService;
    private HDFSService hdfsService;
    private SolrSchemaInfo schemaInfo;

    @Autowired
    public JDBCAPIController(JDBCService jdbcService, CoreService coreService, SearchService searchService, HDFSService hdfsService,
                             SolrSchemaInfo schemaInfo) {
        this.jdbcService    = jdbcService;
        this.coreService    = coreService;
        this.searchService  = searchService;
        this.hdfsService    = hdfsService;
        this.schemaInfo     = schemaInfo;
    }

    public static Object formatColumnObject(Object val, String schemaType) {
        if (val == null || val.equals("null")) {
            return null;
        }
        if (schemaType.equals("boolean")) {
            String b = val.toString().toUpperCase();
            return val instanceof Boolean ? val : b.startsWith("Y") || b.equals("TRUE");
        }
        if (schemaType.equals("int")) {
            return val instanceof Integer ? val : Integer.parseInt(val.toString());
        }
        if (schemaType.equals("date")) {
            return val instanceof Date ? val : DateUtils.getDateFromDateString(val.toString());
        }
        if (schemaType.equals("float")) {
            return val instanceof Float ? val : Float.parseFloat(val.toString());
        }
        if (schemaType.equals("long")) {
            return val instanceof Long ? val : Long.parseLong(val.toString());
        }
        return val;
    }

    public HashMap<String, Object> getFileColumnData(String line, String delimiter, List<String> fields) {
        List<String> fieldValues = Utils.getTokens(line, delimiter);
        HashMap<String, Object> columnData  = new HashMap<String, Object>();
        for(int i = 0; i < fieldValues.size(); i++) {
            columnData.put(fields.get(i), fieldValues.get(i));
        }
        return columnData;
    }

    public HashMap<String, Object> getJDBCColumnData(ResultSet result, SolrCollectionSchemaInfo schemaInfo) throws SQLException {
        HashMap<String, Object> columnData  = new HashMap<String, Object>();
        List<String> columnNames = new ArrayList<String>();
        List<String> columnValues = new ArrayList<String>();

        ResultSetMetaData metaData = result.getMetaData();
        int nCols = metaData.getColumnCount();

        for(int n = 1; n <= nCols; n++) {
            String colName  = metaData.getColumnName(n);
            Object colValue = formatColumnObject(result.getObject(n), schemaInfo.getFieldType(colName));
            columnNames.add(colName);
            columnData.put(colName, colValue);
            columnValues.add(colValue == null ? "null" : colValue.toString());
        }

        columnData.put(FieldNames.ID, jdbcService.constructUUIDStringFromRowEntry(columnValues));
        return columnData;
    }

    public int updateCore(String coreName, BufferedReader br, String delimiter, ResultSet result, boolean useJDBCResultSet,
                          boolean reindex, Long maxRecords, Integer maxDocsBeforeCommit, SolrCollectionSchemaInfo schemaInfo,
                          StringWriter writer) {
        long nAdded = 0, nTotal = 0;
        String line = "";
        List<String> fields = new ArrayList<String>();

        String errorMessage = "None";

        writer.write("Update data stats for core " + coreName + ":\n");
        writer.write("\t# documents processed: ");

        try {
            if (!useJDBCResultSet) {
                line = br.readLine();
                fields = Utils.getTokens(line, delimiter);
            }

            while (useJDBCResultSet ? result.next() : (line = br.readLine()) != null) {
                HashMap<String, Object> docData = useJDBCResultSet ? getJDBCColumnData(result, schemaInfo) :
                                                                     getFileColumnData(line, delimiter, fields);
                // incremental Sqoop import here

                if (reindex || !searchService.recordExists(coreName, docData.get(FieldNames.ID).toString())) {
                    int addSuccess = coreService.solrServerAdd(coreName, coreService.createSolrDocument(docData));
                    if (Response.success(addSuccess)) {
                        nAdded++;
                    }
                }

                nTotal++;
                if (maxDocsBeforeCommit != null && nAdded > 0 && nAdded % maxDocsBeforeCommit == 0) {
                    if (Response.success(coreService.update(coreName))) {
                        writer.write(nAdded + "...");
                    }
                }
                if (maxRecords != null && nTotal > maxRecords) break;
            }
        } catch (SQLException e) {
            errorMessage = e.getMessage();
        } catch (SolrServerException e) {
            errorMessage = e.getMessage();
        } catch (IOException e) {
            errorMessage = e.getMessage();
        }

        writer.write("\t" + nTotal + " records retrieved\n ");
        writer.write("\t" + nAdded + " added, " + (nTotal - nAdded) + " skipped as they already existed.\n");
        writer.write("\tERRORS: " + errorMessage + "\n");

        return coreService.doSolrOperation(coreName, Operations.OPERATION_UPDATE, null, writer);
    }

    @RequestMapping(value="/update/file", method = RequestMethod.GET)
    public ResponseEntity<String> updateSolrIndexFromFile(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                          @RequestParam(value = PARAM_FILE_NAME, required = true) String fileName,
                                                          @RequestParam(value = PARAM_DELIMITER, required = true) String delimiter,
                                                          @RequestParam(value = PARAM_SUGGESTION_CORE, required = true) boolean isSuggestionCore,
                                                          @RequestParam(value = PARAM_REINDEX, required = false) boolean reindex,
                                                          @RequestParam(value = PARAM_MAX_RECORDS, required = false) Long maxRecords,
                                                          @RequestParam(value = PARAM_COMMIT_EVERY_N_DOCS, required = false) Integer maxDocsBeforeCommit) {
        StringWriter writer = new StringWriter();
        if (reindex) {
            coreService.doSolrOperation(coreName, Operations.OPERATION_DELETE, null, writer);
        }

        File inputFile = new File(fileName);
        if (!inputFile.exists()) {
            writer.write("File " + fileName + " does not exist; can not update.");
        } else {
            try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            BufferedReader br  = new BufferedReader(new InputStreamReader(in));
            String line = br.readLine();
            List<String> fields = Utils.getTokens(line, delimiter);

            int updateSuccess = updateCore(coreName, br, delimiter, null, false, reindex, maxRecords,
                                            maxDocsBeforeCommit, null, writer);

                if (Response.success(updateSuccess) && reindex) {
                    coreService.doSolrOperation(coreName, Operations.OPERATION_ADD_INFOFILES,
                                                hdfsService.getInfoFileContents(coreName), writer);
                }
            } catch (IOException e) {
                writer.write("ERROR " + e.getMessage());
            }
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/update/jdbc", method = RequestMethod.GET)
    public ResponseEntity<String> updateSolrIndexFromJDBC(  @RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                                            @RequestParam(value = PARAM_REINDEX, required = false) boolean reindex,
                                                            @RequestParam(value = PARAM_MAX_RECORDS, required = false) Long maxRecords,
                                                            @RequestParam(value = PARAM_COMMIT_EVERY_N_DOCS, required = false) Integer maxDocsBeforeCommit) {
        StringWriter writer = new StringWriter();
        if (reindex) {
            coreService.doSolrOperation(collection, Operations.OPERATION_DELETE, null, writer);
        }

        JDBCData jdbcData = JDBC.getJDBCData(collection);

        if (jdbcData == null) {
            writer.write("ERROR: no JDBC connection data for " + collection + "\n");
        } else {
            try {
                jdbcData.setSQLStatmentToGetCounts();
                ResultSet result = jdbcService.getJDBCResultSet(jdbcData);
                int updateSuccess = updateCore(collection, null, null, result, true, reindex, maxRecords, maxDocsBeforeCommit,
                        schemaInfo.getSchema(collection), writer);

                if (Response.success(updateSuccess) && reindex) {
                    coreService.doSolrOperation(collection, Operations.OPERATION_ADD_INFOFILES,
                            hdfsService.getInfoFileContents(collection), writer);
                }
            } catch (SQLException e) {
                writer.write("ERROR " + e.getMessage());
            }
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
