package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.utils.SolrUtils;
import model.schema.CollectionSchemaInfo;
import model.schema.CollectionsConfig;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.propertyeditors.StringTrimmerEditor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import service.ExportService;
import service.SearchService;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static GatesBigData.utils.Utils.*;

@Controller
@SessionAttributes(value = {ExportAPIController.ATTRIBUTE_COMMAND})
public class ExportAPIController extends APIController {

    public static final String EXPORT_FILENAME      = "file";
    public static final String EXPORT_FILETYPE      = "type";
    public static final String EXPORT_FIELD_INDICES = "fields";
    public static final String EXPORT_FIELDLIST     = "fl";
    public static final String ATTRIBUTE_COMMAND    = "command";

    public static final int PAGE_SIZE = 1000;

    private static Logger logger = Logger.getLogger(ExportAPIController.class);

    private SearchService searchService;
    private ExportService exportCSVService;
    private ExportService exportJSONService;
    private ExportService exportZipService;
    private CollectionsConfig schemaInfo;

    @Autowired
    public ExportAPIController(SearchService searchService, CollectionsConfig schemaInfo,
                               @Qualifier("exportCSVService")  ExportService exportCSVService,
                               @Qualifier("exportJSONService") ExportService exportJSONService,
                               @Qualifier("exportZipService")  ExportService exportZipService) {
        this.searchService = searchService;
        this.exportCSVService = exportCSVService;
        this.exportJSONService = exportJSONService;
        this.exportZipService = exportZipService;
        this.schemaInfo = schemaInfo;
    }

    private void export(String query, String fq, String coreName, List<SolrQuery.SortClause> sortClauses,
                            List<String> fields, ExportService exportService, PrintWriter writer,
                            ServletOutputStream outputStream) throws IOException {

        int start = 0;
        long numDocs = Constants.INVALID_LONG;
        exportService.beginExportWrite(writer);

        do {
            SolrDocumentList docs = searchService.getResultList(coreName, query, fq, sortClauses,
                                                                start, PAGE_SIZE, null);

            if (numDocs == Constants.INVALID_LONG) {
                numDocs = docs.getNumFound();
                exportService.exportHeaderData(numDocs, query, fq, coreName, writer);
                exportService.exportHeaderRow(fields, writer);
            }
            exportService.export(docs, fields, writer);

            start += PAGE_SIZE;
        } while (start < numDocs);

        exportService.endExportWrite(writer, outputStream);
    }

    @InitBinder(ATTRIBUTE_COMMAND)
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(String.class, new StringTrimmerEditor(true));
    }

    private ExportService getExportServiceByFileType(String fileType) {
        if (fileType.contains(Constants.CSV_FILE_EXT)) return exportCSVService;
        if (fileType.contains(Constants.ZIP_FILE_EXT)) return exportZipService;
        if (fileType.contains(Constants.JSON_FILE_EXT)) return exportJSONService;
        return null;
    }

    private String getContentTypeByFileType(String fileType) {
        if (fileType.contains(Constants.CSV_FILE_EXT))  return Constants.CSV_CONTENT_TYPE;
        if (fileType.contains(Constants.ZIP_FILE_EXT))  return Constants.ZIP_CONTENT_TYPE;
        if (fileType.contains(Constants.JSON_FILE_EXT)) return Constants.JSON_CONTENT_TYPE;
        return "";
    }

    private PrintWriter getWriterByContentType(String contentType, HttpServletResponse response)
            throws IOException{
        return contentType.equals(Constants.ZIP_CONTENT_TYPE) ? null : response.getWriter();
    }

    private ServletOutputStream getOutputStreamByContentType(String contentType, HttpServletResponse response)
            throws IOException {
        return contentType.equals(Constants.ZIP_CONTENT_TYPE) ? response.getOutputStream() : null;
    }

    private List<String> getFieldList(CollectionSchemaInfo info, String indicesString, String fl) {
        if (nullOrEmpty(indicesString)) {
            return nullOrEmpty(fl) ? new ArrayList<String>(info.getViewFields()) : getTokens(fl, ",");
        }
        List<String> indices = Arrays.asList(indicesString.substring(1).split(","));
        return info.getFieldNamesSubset(indices, indicesString.startsWith("+"));
    }

    @RequestMapping(value = "/export")
    public void exportSearchResults(@RequestParam(value = EXPORT_FILENAME, required = true) String fileName,
                                    @RequestParam(value = EXPORT_FILETYPE, required = true) String fileType,
                                    @RequestParam(value = EXPORT_FIELD_INDICES, required = false) String indices,
                                    @RequestParam(value = EXPORT_FIELDLIST, required = false) String fl,
                                    @RequestParam(value = PARAM_QUERY, required = true) String query,
                                    @RequestParam(value = PARAM_COLLECTION, required = true) String collection,
                                    @RequestParam(value = PARAM_SORT_FIELD, required = true) String sortInfo,
                                    @RequestParam(value = PARAM_SORT_ORDER, required = false) String sortOrder,
                                    @RequestParam(value = PARAM_FQ, required = false) String fq,
                                    HttpServletResponse response) throws IOException {

        CollectionSchemaInfo info = schemaInfo.getSchema(collection);
        fq = SolrUtils.composeFilterQuery(fq, info);

        List<SolrQuery.SortClause> sortClauses = SolrUtils.getSortClauses(sortInfo, sortOrder);
        List<String> fields = getFieldList(info, indices, fl);

        ExportService exportService = getExportServiceByFileType(fileType);
        exportService.setExportFileName(fileName, fileType);
        String contentType = getContentTypeByFileType(fileType);

        response.reset();
        response.setContentType(contentType);
        response.setHeader(Constants.CONTENT_DISP_HEADER, Constants.getContentDispositionFileAttachHeader(fileName));

        PrintWriter writer = getWriterByContentType(contentType, response);
        ServletOutputStream outputStream = getOutputStreamByContentType(contentType, response);

        long time = System.currentTimeMillis();
        if (query != null) {
            export(query, fq, collection, sortClauses, fields, exportService, writer, outputStream);
        } else {
            exportService.writeEmptyResultSet(writer);
        }

        exportService.closeWriters(writer, outputStream);
        System.out.println("Export time: " + (System.currentTimeMillis() - time));
    }

}
