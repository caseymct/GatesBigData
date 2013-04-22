package GatesBigData.api;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.SolrCollectionSchemaInfo;
import model.SolrRecord;
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

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

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

    private static final Logger logger = Logger.getLogger(DocumentAPIController.class);

    @Autowired
    public DocumentAPIController(HDFSService hdfsService, DocumentConversionService documentConversionService, SearchService searchService) {
        this.hdfsService = hdfsService;
        this.documentConversionService = documentConversionService;
        this.searchService = searchService;
    }

    public SolrRecord getSolrRecord(String coreName, String id, String segment, boolean structuredData) {
        SolrRecord record = null;
        SolrDocument doc = searchService.getRecord(coreName, id);

        if (!Utils.nullOrEmpty(doc)) {
            // then this is a Solr record. Get contents.
            if (structuredData) {
                record = new SolrRecord(doc, searchService.getFieldsToWrite(doc, coreName, Constants.VIEW_TYPE_FULLVIEW));
            } else {
                record = new SolrRecord(doc);
            }
        } else if (!Utils.nullOrEmpty(segment)) {
            record = new SolrRecord(hdfsService.getFileContents(coreName, segment, id), new File(id).getName());
        }

        if (record != null) {
            record.ifRecordIsJSONChangeToText();
            return record;
        }
        return null;
    }

    @RequestMapping(value = "/save", method = RequestMethod.GET)
    public HttpServletResponse saveFile(@RequestParam(value = PARAM_HDFSSEGMENT, required = false) String segment,
                                        @RequestParam(value = PARAM_ID, required = true) String id,
                                        @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                        HttpServletRequest request, HttpServletResponse response) throws IOException {

        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, request.getSession());
        SolrRecord record = getSolrRecord(coreName, id, segment, schemaInfo.isStructuredData());

        response.setContentType(record.getContentType());
        response.setHeader("Content-Disposition", "attachment; fileName=" + record.getFileName());

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
        response.setContentType(Constants.FLASH_CONTENT_TYPE);
        response.setHeader(PRAGMA_HEADER, PRAGMA_VALUE);
        response.setHeader(CACHE_CONTROL_HEADER, CACHE_CONTROL_VALUE);
        response.setHeader(EXPIRES_HEADER, EXPIRES_VALUE);
        response.setHeader(CONNECTION_HEADER, CONNECTION_VALUE);

        BufferedOutputStream output = null;
        BufferedInputStream input = null;

        String convertResponse = documentConversionService.convertDocumentToSwf(filePath);
        try {
            output = new BufferedOutputStream(response.getOutputStream(), DEFAULT_BUFFER_SIZE);

            if (Utils.hasFileErrorMessage(convertResponse)) {
                response.setHeader(Constants.CONTENT_LENGTH_HEADER, Long.toString(convertResponse.length()));
                output.write(convertResponse.getBytes());

            } else {
                File swfFile = new File(convertResponse);
                response.setHeader(Constants.CONTENT_LENGTH_HEADER, Long.toString(swfFile.length()));
                input = new BufferedInputStream(new FileInputStream(swfFile), DEFAULT_BUFFER_SIZE);

                byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
                int length;
                while ((length = input.read(buffer)) > 0) {
                    output.write(buffer, 0, length);
                }
            }
        } finally {
            Utils.closeResource(input);
            Utils.closeResource(output);
        }

        return response;
    }

    private String getViewType(String viewType) {

        if (viewType != null) {
            Matcher m = Constants.VIEW_TYPE_PATTERN.matcher(viewType);
            if (m.matches()) {
                return viewType;
            }
        }
        return Constants.VIEW_TYPE_PREVIEW;
    }

    @RequestMapping(value = "/content/get", method = RequestMethod.GET)
    public ResponseEntity<String> getFileContents(@RequestParam(value = PARAM_ID, required = true) String key,
                                                  @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                  @RequestParam(value = PARAM_VIEWTYPE, required = false) String viewType,
                                                  HttpServletRequest request) throws IOException {
        StringWriter writer = new StringWriter();
        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, request.getSession());

        searchService.printRecord(coreName, key, getViewType(viewType), schemaInfo.isStructuredData(), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/writelocal", method = RequestMethod.GET)
    public ResponseEntity<String> writeLocalFile(@RequestParam(value = PARAM_ID, required = true) String id,
                                                 @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                 @RequestParam(value = PARAM_HDFSSEGMENT, required = false) String segment,
                                                 HttpServletRequest request) throws IOException {

        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, request.getSession());
        StringWriter writer = new StringWriter();

        SolrRecord record = getSolrRecord(coreName, id, segment, schemaInfo.isStructuredData());
        documentConversionService.writeLocalCopy(record.getContent(), record.getFileName(), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/thumbnail/get", method = RequestMethod.GET)
    public ResponseEntity<String> getThumbnail(@RequestParam(value = PARAM_ID, required = true) String id,
                                               @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                               HttpServletRequest request) throws IOException {

        SolrCollectionSchemaInfo schemaInfo = getSolrCollectionSchemaInfo(coreName, request.getSession());
        StringWriter writer = new StringWriter();
        searchService.printRecord(coreName, id, Constants.VIEW_TYPE_PREVIEW, schemaInfo.isStructuredData(), writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}