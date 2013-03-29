package service;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocumentList;

import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

public class ExportCSVServiceImpl extends ExportService {

    private static final Logger logger = Logger.getLogger(ExportCSVServiceImpl.class);

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

    public void exportHeaderRow(List<String> fields, final Writer writer) throws IOException {
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
        solrDocExport(docs, fields, writer);
    }
}
