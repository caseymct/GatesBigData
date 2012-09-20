package LucidWorksApp.api;

import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.HDFSService;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/data")
public class HDFSAPIController extends APIController {

    private static final String PARAM_LOCALFILENAME = "localfile";
    private static final String PARAM_REMOTEFILENAME = "remotefile";
    private static final String PARAM_LOCALDIR = "localdir";
    private static final String PARAM_REMOTEDIR = "remotedir";
    private static final String PARAM_HDFSSEGMENT = "segment";

    private HDFSService hdfsService;
    private static final Logger logger = Logger.getLogger(HDFSAPIController.class);

    @Autowired
    public HDFSAPIController(HDFSService hdfsService) {
        this.hdfsService = hdfsService;
    }

    @RequestMapping(value = "/add", method = RequestMethod.GET)
    public ResponseEntity<String> addRawData(@RequestParam(value = PARAM_LOCALFILENAME, required = true) String localFilename,
                                                 @RequestParam(value = PARAM_REMOTEFILENAME, required = true) String remoteFilename) {

        boolean added = hdfsService.addFile(remoteFilename, localFilename);
        JSONObject success = new JSONObject();
        success.put("Success", added);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(success.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/add/all", method = RequestMethod.GET)
    public ResponseEntity<String> addAllRawData(@RequestParam(value = PARAM_LOCALDIR, required = true) String localDir,
                                             @RequestParam(value = PARAM_REMOTEDIR, required = true) String remoteDir) {

        int added = hdfsService.addAllFilesInLocalDirectory(remoteDir, localDir);
        JSONObject success = new JSONObject();
        success.put("Number of files added", added);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(success.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/read", method = RequestMethod.GET)
    public ResponseEntity<String> readRawData(@RequestParam(value = PARAM_REMOTEFILENAME, required = true) String remoteFile) {

        JSONObject contents = hdfsService.getJSONFileContents(new Path(remoteFile));

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(contents.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/listfiles", method = RequestMethod.GET)
    public ResponseEntity<String> listFiles(@RequestParam(value = PARAM_HDFS, required = true) String hdfsDir) {

        List<String> files = hdfsService.listFiles(new Path(hdfsDir), true);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(StringUtils.join(files, "\n"), httpHeaders, OK);
    }

    @RequestMapping(value = "/remove", method = RequestMethod.GET)
    public ResponseEntity<String> removeFile(@RequestParam(value = PARAM_REMOTEFILENAME, required = true) String remoteFile) {
        boolean removed = hdfsService.removeFile(remoteFile);

        JSONObject success = new JSONObject();
        success.put("Removed file " + remoteFile, removed);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(success.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/nutch", method = RequestMethod.GET)
    public ResponseEntity<String> readNutchFileFromHDFS(@RequestParam(value = PARAM_HDFSSEGMENT, required = true) String segment,
                                                        @RequestParam(value = PARAM_FILENAME, required = true) String key) throws IOException {
        StringWriter writer = new StringWriter();

        hdfsService.printFileContents(segment, key, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public ResponseEntity<String> test() throws IOException {
        hdfsService.testCrawlData("");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>("", httpHeaders, OK);
    }

    @RequestMapping(value = "/nutch/listincrawl", method = RequestMethod.GET)
    public ResponseEntity<String> readNutchFileFromHDFS() throws IOException {
        StringWriter writer = new StringWriter();
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        TreeMap<String, String> filesCrawled = hdfsService.listFilesInCrawlDirectory();

        g.writeStartObject();

        g.writeStringField("Number of files: ", (String) filesCrawled.get("Total"));

        for(Map.Entry<String, String> entry : filesCrawled.entrySet()) {
            g.writeArrayFieldStart(entry.getKey());
            for(String file : entry.getValue().split(",")) {
                g.writeString(file);
            }
            g.writeEndArray();
        }

        g.writeEndObject();
        g.close();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}