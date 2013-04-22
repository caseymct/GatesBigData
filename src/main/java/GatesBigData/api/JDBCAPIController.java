package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.DateUtils;
import GatesBigData.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
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
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/jdbc")
public class JDBCAPIController extends APIController {

    private static final Logger logger = Logger.getLogger(JDBCAPIController.class);

    private static final int MAX_DOCS_BEFORE_COMMIT = 10;

    private JDBCService jdbcService;
    private CoreService coreService;
    private SearchService searchService;
    private HDFSService hdfsService;

    @Autowired
    public JDBCAPIController(JDBCService jdbcService, CoreService coreService, SearchService searchService, HDFSService hdfsService) {
        this.jdbcService    = jdbcService;
        this.coreService    = coreService;
        this.searchService  = searchService;
        this.hdfsService    = hdfsService;
    }

    public static Object formatColumnObject(Object val, String columnTypeName) {
        if (columnTypeName.equals(Constants.SQL_TYPE_STRING) && val instanceof String) {
            java.util.Date d = DateUtils.getDateFromDateString(val.toString());
            return d != null ? new Date(d.getTime()) : val;
        }
        return val;
    }

    public int updateCoreFromFile(String coreName, String fileName, String delimiter, boolean reindex,
                                  Long maxRecords, int maxDocsBeforeCommit, StringWriter writer) {
        File inputFile = new File(fileName);
        if (!inputFile.exists()) {
            writer.write("File " + fileName + " does not exist; can not update.");
            return Constants.SOLR_RESPONSE_CODE_ERROR;
        }

        long nAdded = 0, nTotal = 0;
        String errorMessage = "None";

        writer.write("Update data stats for core " + coreName + ":\n");
        writer.write("\t# documents processed: ");

        try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            BufferedReader br  = new BufferedReader(new InputStreamReader(in));
            String line = br.readLine();
            List<String> fields = Utils.getTokens(line, delimiter);

            while ((line = br.readLine()) != null) {
                List<String> fieldValues = Utils.getTokens(line, delimiter);
                HashMap<String, Object> columnData  = new HashMap<String, Object>();
                for(int i = 0; i < fieldValues.size(); i++) {
                    columnData.put(fields.get(i), fieldValues.get(i));
                }

                if (reindex || !searchService.recordExists(coreName, columnData.get(Constants.SOLR_FIELD_NAME_ID).toString())) {
                    int addSuccess = coreService.solrServerAdd(coreName, coreService.createSolrDocument(columnData));
                    if (Constants.SolrResponseSuccess(addSuccess)) {
                        nAdded++;
                    }
                }
                nTotal++;
                if (maxDocsBeforeCommit != -1 && nAdded > 0 && nAdded % maxDocsBeforeCommit == 0) {
                    if (Constants.SolrResponseSuccess(coreService.update(coreName))) {
                        writer.write(nAdded + "...");
                    }
                }
                if (maxRecords != null && nTotal > maxRecords) break;
                /*int firstIndex    = valueString.indexOf(":"), lastIndex = valueString.lastIndexOf(":");
                String fieldName  = valueString.substring(0, firstIndex);
                String fieldValue = valueString.substring(firstIndex + 1, lastIndex);
                long count = Long.parseLong(valueString.substring(lastIndex + 1));

                SolrInputDocument solrDoc = new SolrInputDocument();
                solrDoc.addField(SOLR_KEY_ID, UUID.randomUUID());
                solrDoc.addField(fieldName, fieldValue);
                solrDoc.addField(SOLR_KEY_COUNT, count);

                try {
                    solrServer.add(solrDoc);
                } */
            }
            in.close();
        } catch (IOException e) {
            errorMessage = e.getMessage();
        } catch (SolrServerException e) {
            errorMessage = e.getMessage();
        }

        writer.write("\t" + nTotal + " records retrieved\n ");
        writer.write("\t" + nAdded + " added, " + (nTotal - nAdded) + " skipped as they already existed.\n");
        writer.write("\tERRORS: " + errorMessage + "\n");

        return coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_UPDATE, null, writer);
    }

    public HashMap<String, Object> getFileColumnData(String line, String delimiter, List<String> fields) {
        List<String> fieldValues = Utils.getTokens(line, delimiter);
        HashMap<String, Object> columnData  = new HashMap<String, Object>();
        for(int i = 0; i < fieldValues.size(); i++) {
            columnData.put(fields.get(i), fieldValues.get(i));
        }
        return columnData;
    }

    public HashMap<String, Object> getJDBCColumnData(ResultSet result) throws SQLException {
        HashMap<String, Object> columnData  = new HashMap<String, Object>();
        List<String> columnNames = new ArrayList<String>();
        ResultSetMetaData metaData = result.getMetaData();
        int nCols = metaData.getColumnCount();

        for(int n = 1; n <= nCols; n++) {
            String colName  = metaData.getColumnName(n);
            Object colValue = formatColumnObject(result.getObject(n), metaData.getColumnTypeName(n));
            columnNames.add(colName);
            columnData.put(colName, colValue);
        }

        String uuidString = jdbcService.constructUUIDStringFromRowEntry(columnNames, columnData);
        columnData.put(Constants.SOLR_FIELD_NAME_ID, uuidString);

        return columnData;
    }

    public int updateCore(String coreName, BufferedReader br, String delimiter, ResultSet result, boolean useJDBCResultSet,
                          boolean reindex, Long maxRecords, Integer maxDocsBeforeCommit, StringWriter writer) {
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
                HashMap<String, Object> docData = useJDBCResultSet ? getJDBCColumnData(result) :
                                                                     getFileColumnData(line, delimiter, fields);

                if (reindex || !searchService.recordExists(coreName, docData.get(Constants.SOLR_FIELD_NAME_ID).toString())) {
                    int addSuccess = coreService.solrServerAdd(coreName, coreService.createSolrDocument(docData));
                    if (Constants.SolrResponseSuccess(addSuccess)) {
                        nAdded++;
                    }
                }
                nTotal++;
                if (maxDocsBeforeCommit != null && nAdded > 0 && nAdded % maxDocsBeforeCommit == 0) {
                    if (Constants.SolrResponseSuccess(coreService.update(coreName))) {
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

        return coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_UPDATE, null, writer);
    }

    public int updateCore(String coreName, ResultSet result, boolean reindex, Long maxRecords,
                          Integer maxDocsBeforeCommit, StringWriter writer) {
        long nAdded = 0, nTotal = 0;
        String errorMessage = "None";

        writer.write("Update data stats for core " + coreName + ":\n");
        writer.write("\t# documents processed: ");

        try {
            ResultSetMetaData metaData = result.getMetaData();
            int nCols = metaData.getColumnCount();

            for(int n = 1; n <= nCols; n++) {
                writer.write(metaData.getColumnName(n) + ",");
            }
            writer.write("\n");

            while (result.next()) {
                HashMap<String, Object> columnData = new HashMap<String, Object>();
                List<String> columnNames = new ArrayList<String>();

                for(int n = 1; n <= nCols; n++) {
                    String colName  = metaData.getColumnName(n);
                    Object colValue = formatColumnObject(result.getObject(n), metaData.getColumnTypeName(n));
                    columnNames.add(colName);
                    columnData.put(colName, colValue);
                    String cvs = colValue == null ? "null" : colValue.toString();
                    if (cvs.contains(",")) {
                        cvs = "\"" + cvs.replaceAll(",", "\\,") + "\"";
                    }
                    writer.write(cvs + ",");
                }
                writer.write("\n");
                /*
                String uuidString = jdbcService.constructUUIDStringFromRowEntry(columnNames, columnData);
                columnData.put(Constants.SOLR_FIELD_NAME_ID, uuidString);

                if (reindex || !searchService.recordExists(coreName, columnData.get(Constants.SOLR_FIELD_NAME_ID).toString())) {
                    int addSuccess = coreService.solrServerAdd(coreName, coreService.createSolrDocument(columnData));
                    if (Constants.SolrResponseSuccess(addSuccess)) {
                        nAdded++;
                    }
                }   */
                nTotal++;
                if (maxDocsBeforeCommit != null && nAdded > 0 && nAdded % maxDocsBeforeCommit == 0) {
                    if (Constants.SolrResponseSuccess(coreService.update(coreName))) {
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

        return 0;//coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_UPDATE, null, writer);
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
            coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_DELETE, null, writer);
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
                                            maxDocsBeforeCommit, writer);


                if (Constants.SolrResponseSuccess(updateSuccess) && reindex) {
                    HashMap<String, String> params = hdfsService.getInfoFilesContents(coreName);
                    coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_ADD_INFOFILES, params, writer);
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
    public ResponseEntity<String> updateSolrIndexFromJDBC(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                          @RequestParam(value = PARAM_REINDEX, required = false) boolean reindex,
                                          @RequestParam(value = PARAM_MAX_RECORDS, required = false) Long maxRecords,
                                          @RequestParam(value = PARAM_COMMIT_EVERY_N_DOCS, required = false) Integer maxDocsBeforeCommit) {
        StringWriter writer = new StringWriter();

        if (reindex) {
            coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_DELETE, null, writer);
        }

        String updateStatement = jdbcService.getUpdateStatement(coreName);
        if (updateStatement == null) {
            writer.write("ERROR: no corresponding update statement for core " + coreName + "\n");
        } else {
            try {
                ResultSet result = jdbcService.getJDBCResultSet(updateStatement);
                int updateSuccess = updateCore(coreName, result, reindex, maxRecords, maxDocsBeforeCommit, writer);

                //int updateSuccess = updateCore(coreName, BufferedReader br, String delimiter, ResultSet result, boolean useJDBCResultSet,
                //boolean reindex, Long maxRecords, Integer maxDocsBeforeCommit, StringWriter writer)

                if (Constants.SolrResponseSuccess(updateSuccess) && reindex) {
                    HashMap<String, String> params = hdfsService.getInfoFilesContents(coreName);
                    coreService.doSolrOperation(coreName, Constants.SOLR_OPERATION_ADD_INFOFILES, params, writer);
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
