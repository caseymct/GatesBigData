package LucidWorksApp.api;

import LucidWorksApp.utils.SolrUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.propertyeditors.StringTrimmerEditor;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.support.SessionStatus;
import service.CoreService;
import service.ExportService;
import service.SearchService;
import service.SolrService;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@SessionAttributes(value = {ExportAPIController.ATTRIBUTE_COMMAND})
public class ExportAPIController extends APIController {

    public static final String EXPORT_FILENAME = "file";
    public static final String EXPORT_FILETYPE = "type";
    public static final String EXPORT_FIELDS = "fields";
    public static final String ATTRIBUTE_COMMAND = "command";

    public static final int PAGE_SIZE = 200;

    private SearchService searchService;
    private ExportService exportCSVService;
    private ExportService exportJSONService;
    private SolrService solrService;
    private CoreService coreService;

    @Autowired
    public ExportAPIController(SearchService searchService, SolrService solrService, CoreService coreService,
                               @Qualifier("exportCSVService") ExportService exportCSVService,
                               @Qualifier("exportJSONService") ExportService exportJSONService) {
        this.searchService = searchService;
        this.solrService = solrService;
        this.coreService = coreService;
        this.exportCSVService = exportCSVService;
        this.exportJSONService = exportJSONService;
    }

    private void exportHeaderData(String query, String fq, String coreName,
                                  String sortType, SolrQuery.ORDER sortOrder, ExportService exportService, PrintWriter writer) throws IOException {

        QueryResponse rsp = searchService.execQuery(query, coreName, SolrUtils.SOLR_SCHEMA_HDFSKEY,
                                                    sortOrder, 0, 0, fq, null);
        exportService.exportHeaderData(rsp.getResults().getNumFound(), query, fq, coreName, writer);
    }

    private void export(boolean onlyJsonContentType, String query, String fq, String coreName,
                        String sortType, SolrQuery.ORDER sortOrder, List<String> fields,
                        ExportService exportService, PrintWriter writer) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{

        fq = (fq == null ? "" : fq) + (onlyJsonContentType ? "+" : "-") + "content_type:\"application/json\"";
        int start = 0;
        long numDocs = -1;

        do {
            QueryResponse rsp = searchService.execQuery(query, coreName, SolrUtils.SOLR_SCHEMA_HDFSKEY,
                                                        sortOrder, start, PAGE_SIZE, fq, null);

            if (numDocs == -1) numDocs = rsp.getResults().getNumFound();

            if (numDocs > 0) {
                if (onlyJsonContentType) {
                    exportService.exportJSONDocs(rsp.getResults(), fields, coreName, writer);
                } else {
                    exportService.export(rsp.getResults(), fields, coreName, writer);
                }
            }
            start += PAGE_SIZE;
        } while (start < numDocs);
    }

    @InitBinder(ATTRIBUTE_COMMAND)
    public void initBinder(WebDataBinder binder) {
        binder.registerCustomEditor(String.class, new StringTrimmerEditor(true));
    }

    @RequestMapping(value = "/export")
    public void exportSearchResults(@RequestParam(value = EXPORT_FILENAME, required = true) String fileName,
                                    @RequestParam(value = EXPORT_FILETYPE, required = true) String fileType,
                                    @RequestParam(value = EXPORT_FIELDS, required = true) String fieldString,
                                    @RequestParam(value = PARAM_QUERY, required = true) String query,
                                    @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                    @RequestParam(value = PARAM_SORT_TYPE, required = true) String sortType,
                                    @RequestParam(value = PARAM_SORT_ORDER, required = true) String sortOrder,
                                    @RequestParam(value = PARAM_FQ, required = true) String fq,
                                    HttpServletResponse response) throws IOException {

        List<String> indices = Arrays.asList(fieldString.substring(1).split(","));
        List<String> fields = SolrUtils.getLukeFieldNames(coreName, indices, fieldString.startsWith("+"));

        if (!fileName.endsWith("." + fileType)) {
            fileName += "." + fileType;
        }
        long time = System.currentTimeMillis();

        ExportService exportService = fileType.equals("csv") ? exportCSVService : exportJSONService;
        String contentType = fileType.equals("csv") ? "text/csv" : "application/json";

        response.reset();
        response.setContentType(contentType);
        response.setHeader("Content-Disposition", "attachment; fileName=" + fileName);
        PrintWriter writer = response.getWriter();

        SolrQuery.ORDER order = searchService.getSortOrder(sortOrder);

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
