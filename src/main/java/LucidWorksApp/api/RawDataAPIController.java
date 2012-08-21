package LucidWorksApp.api;

import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.HDFSService;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/data")
public class RawDataAPIController extends APIController {

    private static final String PARAM_LOCALFILENAME = "localfile";
    private static final String PARAM_REMOTEFILENAME = "remotefile";

    private HDFSService hdfsService;
    private static final Logger logger = Logger.getLogger(RawDataAPIController.class);

    @Autowired
    public RawDataAPIController(HDFSService hdfsService) {
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

    @RequestMapping(value = "/read", method = RequestMethod.GET)
    public ResponseEntity<String> addRawData(@RequestParam(value = PARAM_REMOTEFILENAME, required = true) String remoteFilename) {

        JSONObject contents = hdfsService.getJSONFileContents(remoteFilename);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(CONTENT_TYPE_HEADER, singletonList(CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(contents.toString(), httpHeaders, OK);
    }

}
