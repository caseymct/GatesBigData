package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.ExtendedSolrQuery;
import model.SolrCollectionSchemaInfo;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.CoreService;
import service.HDFSService;
import service.SearchService;
import service.SolrService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/solr")
public class SolrAPIController extends APIController {

    private static final int PAGE_SIZE = 100000;

    public SolrService solrService;
    public CoreService coreService;
    public SearchService searchService;
    public HDFSService hdfsService;

    @Autowired
    public SolrAPIController(SolrService solrService, CoreService coreService, SearchService searchService, HDFSService hdfsService) {
        this.solrService   = solrService;
        this.coreService   = coreService;
        this.hdfsService   = hdfsService;
        this.searchService = searchService;
    }

    private boolean skipCoreName(String coreName) {
        return coreName.equals(Constants.SOLR_THUMBNAILS_CORE_NAME) ||
                coreName.endsWith(Constants.SOLR_SUGGEST_CORE_SUFFIX);
    }

    @RequestMapping(value="/corenames", method = RequestMethod.GET)
    public ResponseEntity<String> getCollectionNames(HttpServletRequest request) throws IOException {

        HttpSession session = request.getSession();

        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeArrayFieldStart("cores");
        for(String name : solrService.getCoreNames()) {
            if (skipCoreName(name)) continue;

            SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(name, session);
            g.writeStartObject();
            g.writeStringField(Constants.SOLR_CORE_FIELD_NAME, name);
            g.writeStringField(Constants.SOLR_TITLE_FIELD_NAME, schemaInfo.getCoreTitle());
            g.writeStringField(Constants.SOLR_STRUCTURED_DATA_FIELD_NAME, schemaInfo.isStructuredData() + "");
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/add/infofiles", method = RequestMethod.GET)
    public ResponseEntity<String> addInfoFiles() {

        JSONObject response = new JSONObject();
        for(String coreName : solrService.getCoreNames()) {
            boolean added = coreService.addInfoFilesToSolr(coreName, hdfsService.getInfoFilesContents(coreName));
            response.put(coreName, added);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(response.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/info/all", method = RequestMethod.GET)
    public ResponseEntity<String> coreInfoAll() throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(solrService.getAllCoreData().toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/suggestions/populate", method = RequestMethod.GET)
    public ResponseEntity<String> populateSuggestions(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName)
            throws IOException {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        StringWriter writer = new StringWriter();

        String suggestionCoreName = SolrUtils.getSuggestionCoreName(coreName);
        SolrServer suggestionSolrServer = coreService.getSolrServer(suggestionCoreName);
        coreService.deleteIndex(suggestionCoreName);

        // Couldn't find a better way to retrieve core information from an empty core
        // JIRA issue indicates that REST functionality has not yet been exposed
        SolrCollectionSchemaInfo schemaInfo = new SolrCollectionSchemaInfo(coreName);
        String viewFields = StringUtils.join(schemaInfo.getPrefixFieldMap().values(), ",") + ",id";
        int start = 0;
        long numDocs = Constants.INVALID_LONG;
        long numAdded = 0;

        do {
            SolrDocumentList docs = searchService.getResultList(coreName, Constants.SOLR_QUERY_DEFAULT, null,
                    Constants.SOLR_SORT_FIELD_DEFAULT, Constants.SOLR_SORT_ORDER_DEFAULT, start, PAGE_SIZE, viewFields);

            if (numDocs == Constants.INVALID_LONG) {
                numDocs = docs.getNumFound();
            }

            List<SolrInputDocument> suggestionDocs = new ArrayList<SolrInputDocument>();
            for(SolrDocument doc : docs) {
                SolrInputDocument suggestionDoc = new SolrInputDocument();

                for(String fieldName : doc.getFieldNames()) {
                    suggestionDoc.addField(fieldName, doc.getFieldValue(fieldName));
                }
                suggestionDocs.add(suggestionDoc);
            }

            try {
                suggestionSolrServer.add(suggestionDocs);
                numAdded += docs.size();
            } catch (SolrServerException e) {
                writer.append("Solr server exception: ").append(e.getMessage());
            }

            start += PAGE_SIZE;
        } while (start < numDocs);

        writer.append("Added ").append(Long.toString(numAdded)).append(" docs\n");
        try {
            UpdateResponse updateResponse = suggestionSolrServer.commit();
            writer.append("Commit returned status ").append(Integer.toString(updateResponse.getStatus()));
            writer.append("Commit returned header ").append(updateResponse.getResponseHeader().toString());
        } catch (SolrServerException e) {
            writer.append("Commit failed! ").append(e.getMessage());
        }

        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}