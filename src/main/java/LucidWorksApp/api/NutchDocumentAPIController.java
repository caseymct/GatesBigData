package LucidWorksApp.api;

import LucidWorksApp.utils.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.DocumentConversionService;
import service.HDFSService;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/document")
public class NutchDocumentAPIController extends APIController {

    private static final int DEFAULT_BUFFER_SIZE = 10240;

    private static final String FLASH_MIME_TYPE = "application/x-shockwave-flash";
    private static final String IMG_MIME_TYPE = "image/png";
    private static final String PRAGMA_HEADER = "Pragma";
    private static final String PRAGMA_VALUE = "public";
    private static final String CACHE_CONTROL_HEADER = "Cache-Control";
    private static final String CACHE_CONTROL_VALUE = "no-cache, must-revalidate";
    private static final String EXPIRES_HEADER = "Expires";
    private static final String EXPIRES_VALUE = "Sat, 26 Jul 1997 05:00:00 GMT";
    private static final String CONNECTION_HEADER = "Connection";
    private static final String CONNECTION_VALUE = "close\r\n\r\n";


    private HDFSService hdfsService;
    private DocumentConversionService documentConversionService;

    private static final Logger logger = Logger.getLogger(NutchDocumentAPIController.class);

    @Autowired
    public NutchDocumentAPIController(HDFSService hdfsService, DocumentConversionService documentConversionService) {
        this.hdfsService = hdfsService;
        this.documentConversionService = documentConversionService;
    }

    @RequestMapping(value = "/save", method = RequestMethod.GET)
    public HttpServletResponse saveFile(@RequestParam(value = PARAM_HDFSSEGMENT, required = true) String segment,
                                        @RequestParam(value = PARAM_FILE_NAME, required = true) String key,
                                        @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                        HttpServletResponse response) throws IOException {

        Content content = hdfsService.getFileContents(coreName, segment, key);
        if (content != null) {
            String fileName = new File(key).getName();
            response.setContentType(content.getContentType());
            response.setHeader("Content-Disposition", "attachment; fileName=" + fileName);

            OutputStream outputStream = response.getOutputStream();
            outputStream.write(content.getContent());
            outputStream.close();
        }

        return response;
    }

    @RequestMapping(value = "/convert", method = RequestMethod.POST)
    public HttpServletResponse getSWFfile(@RequestParam(value = "fileName", required = true) String filePath,
                                          HttpServletResponse response) throws IOException {

        response.reset();
        response.setBufferSize(DEFAULT_BUFFER_SIZE);
        response.setContentType(FLASH_MIME_TYPE);
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
            Utils.closeResource(input);
            Utils.closeResource(output);
        }

        return response;
    }

    @RequestMapping(value = "/nutch/get", method = RequestMethod.GET)
    public ResponseEntity<String> readNutchFileFromHDFS(@RequestParam(value = PARAM_HDFSSEGMENT, required = true) String segment,
                                                        @RequestParam(value = PARAM_FILE_NAME, required = true) String key,
                                                        @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                                        @RequestParam(value = PARAM_VIEWTYPE, required = false) String viewType) throws IOException {
        StringWriter writer = new StringWriter();

        boolean preview = viewType != null && viewType.equals("preview");
        hdfsService.printFileContents(coreName, segment, key, writer, preview);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/writelocal", method = RequestMethod.GET)
    public ResponseEntity<String> writeNutchFileFromHDFS(@RequestParam(value = PARAM_HDFSSEGMENT, required = true) String segment,
                                                         @RequestParam(value = PARAM_FILE_NAME, required = true) String key,
                                                         @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) throws IOException {
        StringWriter writer = new StringWriter();
        Content content = hdfsService.getContent(coreName, segment, key, writer);
        if (content != null) {
            documentConversionService.writeLocalCopy(content, key, writer);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/nutch/listincrawl", method = RequestMethod.GET)
    public ResponseEntity<String> readNutchFileFromHDFS(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) throws IOException {
        StringWriter writer = new StringWriter();

        hdfsService.listFilesInCrawlDirectory(coreName, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/thumbnail/get", method = RequestMethod.GET)
    public ResponseEntity<String> getThumbnail(@RequestParam(value = PARAM_HDFSSEGMENT, required = true) String segment,
                                            @RequestParam(value = PARAM_FILE_NAME, required = true) String key,
                                            @RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) throws IOException {

        StringWriter writer = new StringWriter();
        hdfsService.printFileContents(coreName, segment, key, writer, true);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
