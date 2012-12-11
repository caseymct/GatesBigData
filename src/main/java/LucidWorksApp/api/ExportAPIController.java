package LucidWorksApp.api;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.SolrUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.propertyeditors.StringTrimmerEditor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import service.ExportService;
import service.SearchService;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;


@Controller
@SessionAttributes(value = {ExportAPIController.ATTRIBUTE_COMMAND})
public class ExportAPIController extends APIController {

    public static final String EXPORT_FILENAME   = "file";
    public static final String EXPORT_FILETYPE   = "type";
    public static final String EXPORT_FIELDS     = "fields";
    public static final String ATTRIBUTE_COMMAND = "command";

    public static final int PAGE_SIZE = 200;

    private SearchService searchService;
    private ExportService exportCSVService;
    private ExportService exportJSONService;
    private ExportService exportZipService;

    @Autowired
    public ExportAPIController(SearchService searchService,
                               @Qualifier("exportCSVService")  ExportService exportCSVService,
                               @Qualifier("exportJSONService") ExportService exportJSONService,
                               @Qualifier("exportZipService")  ExportService exportZipService) {
        this.searchService = searchService;
        this.exportCSVService = exportCSVService;
        this.exportJSONService = exportJSONService;
        this.exportZipService = exportZipService;
    }

    private int getNumFound(JSONObject response) {
        return (Integer) JsonParsingUtils.extractJSONProperty(response, Arrays.asList("response", "numFound"), Integer.class, Constants.INVALID_INTEGER);
    }

    private JSONArray getDocs(JSONObject response) {
        return (JSONArray) JsonParsingUtils.extractJSONProperty(response, Arrays.asList("response", "docs"), JSONArray.class, new JSONArray());
    }

    private void exportHeaderData(String query, String fq, String coreName, String sortType,
                                  SolrQuery.ORDER sortOrder, ExportService exportService, PrintWriter writer) throws IOException {

        JSONObject response = searchService.execQuery(query, coreName, SolrUtils.SOLR_SCHEMA_HDFSKEY, sortOrder, 0, 0, fq, null, "");
        int numFound = getNumFound(response);

        exportService.exportHeaderData(numFound, query, fq, coreName, writer);
    }

    private void export(boolean onlyJsonContentType, String query, String fq, String coreName,
                        String sortType, SolrQuery.ORDER sortOrder, List<String> fields,
                        ExportService exportService, PrintWriter writer) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{

        fq = (fq == null ? "" : fq) + (onlyJsonContentType ? "+" : "-") + "content_type:\"" + Constants.JSON_CONTENT_TYPE + "\"";
        int start = 0;
        long numDocs = -1;

        do {
            JSONObject response = searchService.execQuery(query, coreName, SolrUtils.SOLR_SCHEMA_HDFSKEY,
                                                          sortOrder, start, PAGE_SIZE, fq, null, null);
            JSONArray results = getDocs(response);
            if (numDocs == -1) {
                numDocs = getNumFound(response);
            }

            if (numDocs > 0) {
                if (onlyJsonContentType) {
                    //exportService.exportJSONDocs(rsp.getResults(), fields, coreName, writer);
                    exportService.exportJSONDocs(results, fields, coreName, writer);
                } else {
                    //exportService.export(rsp.getResults(), fields, coreName, writer);
                    exportService.export(results, fields, coreName, writer);
                }
            }
            start += PAGE_SIZE;
        } while (start < numDocs);
    }

    @InitBinder(ATTRIBUTE_COMMAND)
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(String.class, new StringTrimmerEditor(true));
    }

    private ExportService getExportServiceByFileType(String fileType) {
        if (fileType.contains("csv")) return exportCSVService;
        if (fileType.contains("zip")) return exportZipService;
        if (fileType.contains("json")) return exportJSONService;
        return null;
    }

    private String getContentTypeByFileType(String fileType) {
        if (fileType.contains("csv"))  return Constants.CSV_CONTENT_TYPE;
        if (fileType.contains("zip"))  return Constants.ZIP_CONTENT_TYPE;
        if (fileType.contains("json")) return Constants.JSON_CONTENT_TYPE;
        return "";
    }

    @RequestMapping(value = "/export")
    public void exportSearchResults(@RequestParam(value = EXPORT_FILENAME, required = true) String fileName,
                                    @RequestParam(value = EXPORT_FILETYPE, required = true) String fileType,
                                    @RequestParam(value = EXPORT_FIELDS, required = true) String fieldString,
                                    @RequestParam(value = PARAM_QUERY, required = true) String query,
                                    @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                    @RequestParam(value = PARAM_SORT_TYPE, required = true) String sortType,
                                    @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                    @RequestParam(value = PARAM_FQ, required = false) String fq,
                                    HttpServletResponse response) throws IOException {

        List<String> indices = Arrays.asList(fieldString.substring(1).split(","));
        List<String> fields = SolrUtils.getLukeFieldNames(coreName, indices, fieldString.startsWith("+"));

        if (!fileName.endsWith("." + fileType)) {
            fileName += "." + fileType;
        }
        long time = System.currentTimeMillis();

        ExportService exportService = getExportServiceByFileType(fileType);
        String contentType = getContentTypeByFileType(fileType);

        response.reset();
        response.setContentType(contentType);
        response.setHeader("Content-Disposition", "attachment; fileName=" + fileName);
        PrintWriter writer = response.getWriter();

        SolrQuery.ORDER order = SolrUtils.getSortOrder(sortOrder);

        if (query != null) {
            try {
                exportHeaderData(query, fq, coreName, sortType, order, exportService, writer);
                export(true, query, fq, coreName, sortType, order, fields, exportService, writer);
                exportService.writeDefaultNewline(writer);
                export(false, query, fq, coreName, sortType, order, fields, exportService, writer);

            } catch (InvocationTargetException e) {
                System.out.println(e.getMessage());
            } catch (NoSuchMethodException e) {
                System.out.println(e.getMessage());
            } catch (IllegalAccessException e) {
                System.out.println(e.getMessage());
            }
        } else {
            exportService.writeEmptyResultSet(writer);
        }

        exportService.closeWriters(writer);
        System.out.println("EXPORT TIME " + (System.currentTimeMillis() - time));
    }

}
