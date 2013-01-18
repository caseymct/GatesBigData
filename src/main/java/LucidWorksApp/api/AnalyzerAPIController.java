package LucidWorksApp.api;

import LucidWorksApp.utils.Constants;
import model.SnippetAnalyzer;
import model.WordNode;
import model.WordTree;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.springframework.http.HttpStatus.OK;

@Controller
@RequestMapping("/analyze")
public class AnalyzerAPIController extends APIController {

    @RequestMapping(value="/snippets", method = RequestMethod.POST)
    public ResponseEntity<String> analyze(@RequestBody String body) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = mapper.readValue(body, TypeFactory.mapType(HashMap.class, String.class, Object.class));

        StringWriter writer = new StringWriter();
        String hlResponse = (String) map.get(Constants.SOLR_HIGHLIGHT_PARAM);
        SnippetAnalyzer analyzer = new SnippetAnalyzer(hlResponse);

        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);
        g.writeStartObject();

        for(Map.Entry<String, List<String>> entry : analyzer.getHighlighting().entrySet()) {
            String field = entry.getKey();
            WordTree tree = new WordTree(field, analyzer.getHlPre(), analyzer.getHlPost());

            for(String snippet : entry.getValue()) {
                tree.addSnippet(snippet);
            }
            tree.combine();
            tree.printTree(g);
        }

        g.writeEndObject();
        g.close();
        g.flush();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }

    @RequestMapping(value="/wordtreetest", method = RequestMethod.GET)
    public ResponseEntity<String> test() throws IOException {

        WordTree tree = new WordTree("test", "<b>", "</b>");

        tree.addSnippet("here is a <b>test</b>ing one two three");
        tree.addSnippet("what is a <b>test</b>: one four five three");
        tree.addSnippet("this isn't a <b>test</b> one four five seven");
        tree.addSnippet("why <b>test</b> three four five three");
        tree.addSnippet("<b>test</b> three four three");
        StringWriter writer = new StringWriter();

        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);
        g.writeStartObject();

        tree.combine();
        tree.printTree(g);

        g.writeEndObject();
        g.close();
        g.flush();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.put(Constants.CONTENT_TYPE_HEADER, singletonList(Constants.CONTENT_TYPE_VALUE));
        return new ResponseEntity<String>(writer.toString(), httpHeaders, OK);
    }
}
