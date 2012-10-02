package service;

import net.sf.json.JSONObject;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;

public class ExportCSVServiceImpl implements ExportService {

    private static final Logger logger = Logger.getLogger(ExportCSVServiceImpl.class);
    private HDFSService hdfsService;
    private SolrService solrService;

    @Autowired
    public void setServices(HDFSService hdfsService, SolrService solrService) {
        this.hdfsService = hdfsService;
        this.solrService = solrService;
    }

    public void export(SolrDocumentList docs, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        export(docs, coreName, writer, DEFAULT_DELIMETER, DEFAULT_NEWLINE);
    }

    public void exportHeaderData(long numDocs, String solrParams, final Writer writer) {
        exportHeaderData(numDocs, solrParams, writer, DEFAULT_DELIMETER, DEFAULT_NEWLINE);
    }

    public void exportHeaderData(long numDocs, String solrParams, final Writer writer, String delimiter, String newLine) {
        try {
            writer.append("# Found").append(delimiter).append("Solr Query").append(newLine);
            writer.append(Long.toString(numDocs)).append(delimiter);
            writer.append("\"").append(solrParams).append("\"").append(newLine);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void closeWriters(final Writer writer) {
        try {
            writer.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeEmptyResultSet(final Writer writer) {
        try {
            writer.append("No search results.");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void export(SolrDocumentList docs, String coreName, final Writer writer, String delimeter, String newLine) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        logger.debug("Exporting to CSV");

        // build the csv header row
        List<String> fields = solrService.getFieldNamesFromLuke();
        writer.append(StringUtils.join(fields, delimeter));
        writer.append(newLine);

        HashMap<String, List<String>> segToFileMap = solrService.getSegmentToFilesMap(docs);

        for (Map.Entry<String, List<String>> entry : segToFileMap.entrySet()) {
            for (Content content : hdfsService.getFileContents(coreName, entry.getKey(), entry.getValue())) {

                if (content == null) {
                    continue;
                }

                String contentType = content.getContentType();
                if (contentType.equals("application/json")) {
                    JSONObject jsonObject = JSONObject.fromObject(new String(content.getContent()));
                    Iterator<String> fieldIterator = fields.iterator();
                    while(fieldIterator.hasNext()) {
                        String field = fieldIterator.next();
                        Object val = new Object();
                        JSONObject j = jsonObject;
                        for(String subField : field.split("\\.")) {
                            val = j.get(subField);
                            if (val instanceof JSONObject) {
                                j = (JSONObject) val;
                            }
                        }
                        writer.append(StringEscapeUtils.escapeCsv(val != null ? new String(val.toString().getBytes(), Charset.forName("UTF-8")) : ""));
                        if(fieldIterator.hasNext()) {
                            writer.append(delimeter);
                        }
                    }
                    writer.append(newLine);
                }
            }
        }
        writer.flush();
    }
}
