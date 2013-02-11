package service;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.*;

public class ExportCSVServiceImpl extends ExportService {

    private static final Logger logger = Logger.getLogger(ExportCSVServiceImpl.class);
    private HDFSService hdfsService;

    @Autowired
    public void setServices(HDFSService hdfsService) {
        this.hdfsService = hdfsService;
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer) throws IOException {
        query = "\"" + query.replaceAll("\"", "\"\"") + "\"";
        fq = (fq == null) ? "" : "\"" + fq.replaceAll("\"", "\"\"") + "\"";

        writer.append(NUM_FOUND_HDR).append(delimiter).append(SOLR_QUERY_HDR).append(delimiter);
        writer.append(CORE_NAME_HDR).append(delimiter).append(FILTER_QUERY_HDR).append(delimiter);
        writer.append(newLine);

        writer.append(Long.toString(numDocs)).append(delimiter).append(query).append(delimiter);
        writer.append(coreName).append(delimiter).append(fq).append(delimiter);
        writer.append(newLine).append(newLine);
    }

    public void writeEmptyResultSet(final Writer writer) throws IOException {
        writer.append("No search results.");
    }

    private void buildCSVHeaderRow(List<String> fields, final Writer writer) throws IOException {
        writer.append(StringUtils.join(fields, delimiter));
        writer.append(newLine);
    }

    public void beginExportWrite(Writer writer) throws IOException {}
    public void endExportWrite(Writer writer, ServletOutputStream outputStream) throws IOException {}
    public void beginDocWrite(Writer writer) throws IOException {}

    public void endDocWrite(Writer writer) throws IOException {
        writer.append(newLine);
    }

    public void write(String field, String value, boolean lastField, Writer writer) throws IOException {
        writer.append(value);
        if (!lastField) {
            writer.append(delimiter);
        }
    }

    /*private void _export(SolrDocumentList docs, List<String> fields, final Writer writer, String delimeter, String newLine)
            throws IOException {
        // build the csv header row
        buildCSVHeaderRow(fields, newLine, delimeter, writer);

        solrDocExport(docs, fields, newLine, writer);
        for(SolrDocument doc : docs) {
            for(int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                String val = doc.containsKey(field) ? Utils.getUTF8String(doc.getFieldValue(field).toString()) : "";
                writer.append(StringEscapeUtils.escapeCsv(val));

                if (i < fields.size()) {
                    writer.append(delimeter);
                }
            }
            writer.append(newLine);
        }

        writer.flush();
    }          */

    public void export(SolrDocumentList docs, List<String> fields, final Writer writer) throws IOException {
        logger.debug("Exporting to CSV");
        buildCSVHeaderRow(fields, writer);
        solrDocExport(docs, fields, writer);
    }


    private void exportJSONDocsFromHDFS(SolrDocumentList docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws IOException {
        logger.debug("Exporting JSON docs to CSV");

        writer.append("Documents with Content-type : ").append(Constants.JSON_CONTENT_TYPE).append(newLine);

        buildCSVHeaderRow(fields, writer);

        HashMap<String, List<String>> segToFileMap = SolrUtils.getSegmentToFilesMap(docs);

        for (Map.Entry<String, List<String>> entry : segToFileMap.entrySet()) {
            for (Content content : hdfsService.getFileContents(coreName, entry.getKey(), entry.getValue())) {

                if (content == null || !content.getContentType().equals(Constants.JSON_CONTENT_TYPE)) {
                    continue;
                }

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
        writer.flush();
    }
}
