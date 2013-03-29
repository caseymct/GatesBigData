package GatesBigData.api;

import GatesBigData.utils.Constants;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.HDFSService;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/data")
public class HDFSAPIController extends APIController {

    private static final String PARAM_LOCALFILENAME  = "localfile";
    private static final String PARAM_REMOTEFILENAME = "remotefile";
    private static final String PARAM_LOCALDIR       = "localdir";
    private static final String PARAM_REMOTEDIR      = "remotedir";

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
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(success.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/add/all", method = RequestMethod.GET)
    public ResponseEntity<String> addAllRawData(@RequestParam(value = PARAM_LOCALDIR, required = true) String localDir,
                                                @RequestParam(value = PARAM_REMOTEDIR, required = true) String remoteDir) {

        int added = hdfsService.addAllFilesInLocalDirectory(remoteDir, localDir);
        JSONObject success = new JSONObject();
        success.put("Number of files added", added);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(success.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/listfiles", method = RequestMethod.GET)
    public ResponseEntity<String> listFiles(@RequestParam(value = PARAM_HDFS, required = true) String hdfsDir) {

        List<String> files = hdfsService.listFiles(new Path(hdfsDir), true, null, null);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(StringUtils.join(files, "\n"), httpHeaders, OK);
    }

    @RequestMapping(value = "/nutch/listincrawl", method = RequestMethod.GET)
    public ResponseEntity<String> listInCrawl(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName,
                                              @RequestParam(value = PARAM_INC_PARSE_INFO, required = false) boolean includeParseInfo)
                    throws IOException {
        StringWriter writer = new StringWriter();

        hdfsService.listFilesInCrawlDirectory(coreName, writer, includeParseInfo);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/nutch/listfetched", method = RequestMethod.GET)
    public ResponseEntity<String> listFetched(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName)
            throws IOException {
        StringWriter writer = new StringWriter();

        hdfsService.writeFetched(coreName, writer);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/remove", method = RequestMethod.GET)
    public ResponseEntity<String> removeFile(@RequestParam(value = PARAM_REMOTEFILENAME, required = true) String remoteFile) {
        boolean removed = hdfsService.removeFile(remoteFile);

        JSONObject success = new JSONObject();
        success.put("Removed file " + remoteFile, removed);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(success.toString(), httpHeaders, OK);
    }

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public ResponseEntity<String> test(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) throws IOException {
        hdfsService.testCrawlData(coreName, "");

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>("", httpHeaders, OK);
    }
}
