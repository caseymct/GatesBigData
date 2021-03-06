package GatesBigData.api;

import GatesBigData.constants.Constants;
import GatesBigData.constants.solr.Solr;
import GatesBigData.utils.Utils;
import model.schema.CollectionConfig;
import model.schema.CollectionSchemaInfo;
import model.SolrRecord;
import model.schema.CollectionsConfig;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.DocumentConversionService;
import service.HDFSService;
import service.SearchService;

import javax.servlet.http.HttpServletResponse;
import java.io.*;

import static GatesBigData.utils.Utils.*;
import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;
import static GatesBigData.constants.Constants.*;

@Controller
@RequestMapping("/document")
public class DocumentAPIController extends APIController {

    private static final int DEFAULT_BUFFER_SIZE = 10240;

    private static final String PRAGMA_HEADER           = "Pragma";
    private static final String PRAGMA_VALUE            = "public";
    private static final String CACHE_CONTROL_HEADER    = "Cache-Control";
    private static final String CACHE_CONTROL_VALUE     = "no-cache, must-revalidate";
    private static final String EXPIRES_HEADER          = "Expires";
    private static final String EXPIRES_VALUE           = "Sat, 26 Jul 1997 05:00:00 GMT";
    private static final String CONNECTION_HEADER       = "Connection";
    private static final String CONNECTION_VALUE        = "close\r\n\r\n";

    private HDFSService hdfsService;
    private SearchService searchService;
    private DocumentConversionService documentConversionService;
    private CollectionsConfig collectionsConfig;

    private static final Logger logger = Logger.getLogger(DocumentAPIController.class);

    @Autowired
    public DocumentAPIController(HDFSService hdfsService, DocumentConversionService documentConversionService,
                                 SearchService searchService, CollectionsConfig collectionsConfig) {
        this.hdfsService = hdfsService;
        this.documentConversionService = documentConversionService;
        this.searchService = searchService;
        this.collectionsConfig = collectionsConfig;
    }

    public SolrRecord getSolrRecord(String coreName, String id, String segment) {
        SolrRecord record = null;
        SolrDocument doc = searchService.getRecordById(coreName, id);

        if (!nullOrEmpty(doc)) {
            // then this is a Solr record. Get contents.
            record = new SolrRecord(doc);

        } else if (!nullOrEmpty(segment)) {
            record = new SolrRecord(hdfsService.getFileContents(coreName, segment, id), new File(id).getName());
        }

        return record != null ? record.modifyForWriting() : null;
    }

    @RequestMapping(value = "/save", method = RequestMethod.GET)
    public HttpServletResponse saveFile(@RequestParam(value = PARAM_HDFSSEGMENT, required = false) String segment,
                                        @RequestParam(value = PARAM_ID, required = true) String id,
                                        @RequestParam(value = PARAM_CORE_NAME, required = true) String collectionName,
                                        HttpServletResponse response) throws IOException {

        SolrRecord record = getSolrRecord(collectionName, id, segment);
        response.setContentType(record.getContentType());
        response.setHeader(CONTENT_DISP_HEADER, getContentDispositionFileAttachHeader(record.getFileName()));

        OutputStream outputStream = response.getOutputStream();
        outputStream.write(record.getContent());
        outputStream.close();

        return response;
    }

    @RequestMapping(value = "/convert", method = RequestMethod.POST)
    public HttpServletResponse getSWFfile(@RequestParam(value = "fileName", required = true) String filePath,
                                          HttpServletResponse response) throws IOException {

        response.reset();
        response.setBufferSize(DEFAULT_BUFFER_SIZE);
        response.setContentType(FLASH_CONTENT_TYPE);
        response.setHeader(PRAGMA_HEADER, PRAGMA_VALUE);
        response.setHeader(CACHE_CONTROL_HEADER, CACHE_CONTROL_VALUE);
        response.setHeader(EXPIRES_HEADER, EXPIRES_VALUE);
        response.setHeader(CONNECTION_HEADER, CONNECTION_VALUE);

        BufferedOutputStream output = null;
        BufferedInputStream input = null;

        String convertResponse = documentConversionService.convertDocumentToSwf(filePath);
        try {
            output = new BufferedOutputStream(response.getOutputStream(), DEFAULT_BUFFER_SIZE);

            if (hasFileErrorMessage(convertResponse)) {
                response.setHeader(CONTENT_LENGTH_HEADER, Long.toString(convertResponse.length()));
                output.write(convertResponse.getBytes());

            } else {
                File swfFile = new File(convertResponse);
                response.setHeader(CONTENT_LENGTH_HEADER, Long.toString(swfFile.length()));
                input = new BufferedInputStream(new FileInputStream(swfFile), DEFAULT_BUFFER_SIZE);

                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                int length;
                while ((length = input.read(buffer)) > 0) {
                    output.write(buffer, 0, length);
                }
            }
        } finally {
            closeResource(input);
            closeResource(output);
        }

        return response;
    }

    @RequestMapping(value = "/content/get", method = RequestMethod.GET)
    public ResponseEntity<String> getFileContents(@RequestParam(value = PARAM_ID, required = true) String key,
                                                  @RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                                  @RequestParam(value = PARAM_VIEWTYPE, required = false) String viewType)
                                                  throws IOException {
        StringWriter writer = new StringWriter();

        searchService.printRecord(collection, key, collectionsConfig.getFieldsByType(collection, viewType),
                collectionsConfig.collectionHasStructuredData(collection), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/writelocal", method = RequestMethod.GET)
    public ResponseEntity<String> writeLocalFile(@RequestParam(value = PARAM_ID, required = true) String id,
                                                 @RequestParam(value = PARAM_CORE_NAME, required = true) String collection,
                                                 @RequestParam(value = PARAM_HDFSSEGMENT, required = false) String segment) throws IOException {

        StringWriter writer = new StringWriter();
        SolrRecord record = getSolrRecord(collection, id, segment);
        documentConversionService.writeLocalCopy(record.getContent(), record.getFileName(), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/thumbnail/get", method = RequestMethod.GET)
    public ResponseEntity<String> getThumbnail(@RequestParam(value = PARAM_ID, required = true) String id,
                                               @RequestParam(value = PARAM_CORE_NAME, required = true) String collection)
                                                throws IOException {

        StringWriter writer = new StringWriter();
        CollectionConfig config = collectionsConfig.getCollectionConfig(collection);

        searchService.printRecord(collection, id, config.getPreviewFields(),
                collectionsConfig.collectionHasStructuredData(collection), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
