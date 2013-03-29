package GatesBigData.api;

import GatesBigData.utils.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import service.JDBCService;

import java.io.StringWriter;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/jdbc")
public class JDBCAPIController extends APIController {

    private JDBCService jdbcService;

    @Autowired
    public JDBCAPIController(JDBCService jdbcService) {
        this.jdbcService = jdbcService;
    }

    @RequestMapping(value="/connect", method = RequestMethod.GET)
    public ResponseEntity<String> connect(@RequestParam(value = PARAM_CORE_NAME, required = true) String coreName) {
        StringWriter writer = new StringWriter();

        jdbcService.getData(coreName);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
