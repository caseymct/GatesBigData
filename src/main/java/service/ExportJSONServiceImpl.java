package service;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.SolrUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;

@Service
public class ExportJSONServiceImpl extends ExportService {
    private HDFSService hdfsService;
    private SolrService solrService;
    private JsonGenerator g;

    @Autowired
    public void setServices(HDFSService hdfsService, SolrService solrService) {
        this.hdfsService = hdfsService;
        this.solrService = solrService;
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer, String delimiter, String newLine) {
        try {
            JsonFactory f = new JsonFactory();
            g = f.createJsonGenerator(writer);
            g.setPrettyPrinter(new DefaultPrettyPrinter());
            g.writeStartObject();

            g.writeNumberField("Num found", numDocs);
            g.writeStringField("Core name", coreName);
            g.writeStringField("Query", query);
            g.writeStringField("Filter Query", fq == null ? "" : fq);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void closeWriters(final Writer writer) {
        try {
            g.writeEndObject();
            g.close();

            writer.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeEmptyResultSet(final Writer writer) {
        try {
            JsonFactory f = new JsonFactory();
            g = f.createJsonGenerator(writer);
            g.setPrettyPrinter(new DefaultPrettyPrinter());
            g.writeStartObject();
            g.writeNumberField("Num found", 0);
            g.writeArrayFieldStart("Results");
            g.writeEndArray();
            g.flush();

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void exportJSONDocs(SolrDocumentList docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {

        g.writeArrayFieldStart("JSON document results");
        g.flush();
        writer.append(newLine);

        HashMap<String, List<String>> segToFileMap = SolrUtils.getSegmentToFilesMap(docs);

        for (Map.Entry<String, List<String>> entry : segToFileMap.entrySet()) {
            for (Content content : hdfsService.getFileContents(coreName, entry.getKey(), entry.getValue())) {
                if (content == null || !(content.getContentType().equals(Constants.JSON_CONTENT_TYPE))) {
                    continue;
                }

                JSONObject jsonObject = JSONObject.fromObject(new String(content.getContent()));
                writer.append(jsonObject.toString(2)).append(newLine);
            }
        }
        g.writeEndArray();
        g.flush();
        writer.flush();
    }

    public void export(SolrDocumentList docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        g.writeArrayFieldStart("Non-JSON document results");

        //fields = FIELDS_TO_EXPORT;

        for (SolrDocument doc : docs) {
            g.writeStartObject();

            for(String field : fields) {
                Object val = doc.getFieldValue(field);
                g.writeStringField(field, StringEscapeUtils.escapeCsv(val != null ? new String(val.toString().getBytes(), Charset.forName("UTF-8")) : ""));
            }

            g.writeEndObject();
        }
        g.writeEndArray();
    }
}
