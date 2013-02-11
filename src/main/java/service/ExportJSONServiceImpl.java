package service;

import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.springframework.stereotype.Service;

import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

@Service
public class ExportJSONServiceImpl extends ExportService {

    private JsonGenerator g;

    /*
    private HDFSService hdfsService;
    @Autowired
    public void setServices(HDFSService hdfsService) {
        this.hdfsService = hdfsService;
    } */

    public void beginExportWrite(Writer writer) throws IOException {
        JsonFactory f = new JsonFactory();
        g = f.createJsonGenerator(writer);
        g.setPrettyPrinter(new DefaultPrettyPrinter());
        g.writeStartObject();
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer) throws IOException {
        g.writeNumberField(NUM_FOUND_HDR, numDocs);
        g.writeStringField(CORE_NAME_HDR, coreName);
        g.writeStringField(QUERY_KEY, query);
        g.writeStringField(FILTER_QUERY_HDR, fq == null ? "" : fq);
    }

    public void writeEmptyResultSet(final Writer writer) throws IOException {
        g.writeNumberField(NUM_FOUND_HDR, 0);
        g.writeArrayFieldStart(RESULTS_HDR);
        g.writeEndArray();
        g.flush();
    }

    public void beginDocWrite(Writer writer) throws IOException {
        g.writeStartObject();
    }

    public void endDocWrite(Writer writer) throws IOException {
        g.writeEndObject();
    }

    public void write(String field, String value, boolean lastField, Writer writer) throws IOException {
        g.writeStringField(field, value);
    }

    public void endExportWrite(Writer writer, ServletOutputStream outputStream) throws IOException {
        g.writeEndObject();
        g.close();
    }

    public void export(SolrDocumentList docs, List<String> fields, final Writer writer) throws IOException {
        g.writeArrayFieldStart(RESULTS_HDR);
        solrDocExport(docs, fields, writer);
        g.writeEndArray();
    }

    /*for(SolrDocument doc : docs) {
       g.writeStartObject();

       for(String field : fields) {
           Object val = doc.get(field);
           g.writeStringField(field, StringEscapeUtils.escapeCsv(val != null ? new String(val.toString().getBytes(), Charset.forName("UTF-8")) : ""));
       }

       g.writeEndObject();
   } */
}
