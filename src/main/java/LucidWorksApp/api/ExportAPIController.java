package LucidWorksApp.api;

import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.PrettyPrinter;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import service.ExportService;
import service.SearchService;
import service.SolrService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

@Controller
public class ExportAPIController extends APIController {

    public static final String EXPORT_FILENAME = "file";
    public static final String EXPORT_FILETYPE = "type";

    public static final int PAGE_SIZE = 200;

    private SearchService searchService;
    private ExportService exportCSVService;
    private ExportService exportJSONService;
    private SolrService solrService;

    @Autowired
    public ExportAPIController(SearchService searchService, SolrService solrService,
                               @Qualifier("exportCSVService") ExportService exportCSVService,
                               @Qualifier("exportJSONService") ExportService exportJSONService) {
        this.searchService = searchService;
        this.solrService = solrService;
        this.exportCSVService = exportCSVService;
        this.exportJSONService = exportJSONService;
    }

    private void exportHeaderData(HashMap<String, String> searchParams, ExportService exportService, PrintWriter writer) throws IOException {
        String query = searchParams.get(SESSION_SEARCH_QUERY), fq = searchParams.get(SESSION_SEARCH_FQ),
               coreName = searchParams.get(SESSION_SEARCH_CORE_NAME);

        QueryResponse rsp = searchService.execQuery(query, coreName, solrService.getSolrSchemaHDFSKey(),
                                                    SolrQuery.ORDER.asc, 0, 0, fq, null);
        exportService.exportHeaderData(rsp.getResults().getNumFound(), query, fq, coreName, writer);
    }

    private void export(boolean onlyJsonContentType, HashMap<String, String> searchParams,
                        ExportService exportService, PrintWriter writer) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException{
        String fq = searchParams.get(SESSION_SEARCH_FQ);
        fq = (fq == null ? "" : fq) + (onlyJsonContentType ? "+" : "-") + "content_type:\"application/json\"";
        int start = 0;
        long numDocs = -1;

        do {
            QueryResponse rsp = searchService.execQuery(searchParams.get(SESSION_SEARCH_QUERY),
                    searchParams.get(SESSION_SEARCH_CORE_NAME), solrService.getSolrSchemaHDFSKey(),
                    SolrQuery.ORDER.asc, start, PAGE_SIZE, fq, null);

            if (numDocs == -1) numDocs = rsp.getResults().getNumFound();

            if (numDocs > 0) {
                if (onlyJsonContentType) {
                    exportService.exportJSONDocs(rsp.getResults(), searchParams.get(SESSION_SEARCH_CORE_NAME), writer);
                } else {
                    exportService.export(rsp.getResults(), searchParams.get(SESSION_SEARCH_CORE_NAME), writer);
                }
            }
            start += PAGE_SIZE;
        } while (start < numDocs);
    }

    @RequestMapping("/export")
    public void exportSearchResults(@RequestParam(value = EXPORT_FILENAME, required = true) String fileName,
                                    @RequestParam(value = EXPORT_FILETYPE, required = true) String fileType,
                                    HttpServletRequest request, HttpServletResponse response) throws IOException {

        if (!fileName.endsWith("." + fileType)) {
            fileName += "." + fileType;
        }
        long time = System.currentTimeMillis();

        ExportService exportService = fileType.equals("csv") ? exportCSVService : exportJSONService;
        String contentType = fileType.equals("csv") ? "text/csv" : "application/json";

        response.setContentType(contentType);
        response.setHeader("Content-Disposition", "attachment; fileName=" + fileName);
        PrintWriter writer = response.getWriter();

        HttpSession session = request.getSession();
        HashMap<String, String> searchParams = (HashMap<String,String>) session.getAttribute(SESSION_SEARCH_PARAMS);

        if (searchParams != null) {
            try {
                exportHeaderData(searchParams, exportService, writer);
                export(true, searchParams, exportService, writer);
                exportService.writeDefaultNewline(writer);
                export(false, searchParams, exportService, writer);

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
