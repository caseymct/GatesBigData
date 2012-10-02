package service;

import net.sf.json.JSONObject;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.PrettyPrinter;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;

@Service
public class ExportJSONServiceImpl implements ExportService {
    private HDFSService hdfsService;
    private SolrService solrService;
    private JsonGenerator g;

    @Autowired
    public void setServices(HDFSService hdfsService, SolrService solrService) {
        this.hdfsService = hdfsService;
        this.solrService = solrService;
    }

    public void exportHeaderData(long numDocs, String solrParams, final Writer writer) {
        exportHeaderData(numDocs, solrParams, writer, DEFAULT_DELIMETER, DEFAULT_NEWLINE);
    }

    public void exportHeaderData(long numDocs, String solrParams, final Writer writer, String delimiter, String newLine) {
        try {
            JsonFactory f = new JsonFactory();
            g = f.createJsonGenerator(writer);
            g.setPrettyPrinter(new DefaultPrettyPrinter());
            g.writeStartObject();

            g.writeNumberField("Num found", numDocs);
            g.writeStringField("Query parameters", solrParams);
            g.writeArrayFieldStart("Results");

            g.flush();
            writer.append(newLine);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void closeWriters(final Writer writer) {
        try {
            g.writeEndArray();
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
            g.flush();

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void export(SolrDocumentList docs, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        export(docs, coreName, writer, DEFAULT_DELIMETER, DEFAULT_NEWLINE);
    }

    public void export(SolrDocumentList docs, String coreName, final Writer writer, String delimeter, String newLine) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {

        HashMap<String, List<String>> segToFileMap = solrService.getSegmentToFilesMap(docs);

        for (Map.Entry<String, List<String>> entry : segToFileMap.entrySet()) {
            for (Content content : hdfsService.getFileContents(coreName, entry.getKey(), entry.getValue())) {
                if (content == null) {
                    continue;
                }

                String contentType = content.getContentType();
                if (contentType.equals("application/json")) {
                    JSONObject jsonObject = JSONObject.fromObject(new String(content.getContent()));
                    writer.append(jsonObject.toString(2)).append(newLine);
                }
            }
        }

        writer.flush();
    }
}
