package service;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.Utils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.List;


public abstract class ExportService {
    public static final String RESULTS_HDR         = "Results";
    public static final String NUM_FOUND_HDR       = "# Found";
    public static final String CORE_NAME_HDR       = "Core Name";
    public static final String QUERY_KEY           = "Query";
    public static final String FILTER_QUERY_HDR    = "Filter Query";
    public static final String SOLR_QUERY_HDR      = "Solr Query";

    public String exportFileName = "";
    public String newLine        = Constants.DEFAULT_NEWLINE;
    public String delimiter      = Constants.DEFAULT_DELIMETER;
    public Logger logger         = Logger.getLogger(ExportService.class);

    public void setNewLine(String newLine) {
        this.newLine = newLine;
    }

    public String getNewLine() {
        return this.newLine;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return this.delimiter;
    }

    public void setExportFileName(String exportFileName) {
        this.exportFileName = exportFileName;
    }

    public String getExportFileName() {
        return this.exportFileName;
    }

    public abstract void beginExportWrite(Writer writer) throws IOException;

    public abstract void endExportWrite(Writer writer, ServletOutputStream outputStream) throws IOException;

    public abstract void write(String field, String value, boolean lastField, Writer writer) throws IOException;

    public abstract void beginDocWrite(Writer writer) throws IOException;

    public abstract void endDocWrite(Writer writer) throws IOException;

    public void solrDocExport(SolrDocumentList docs, List<String> fields, Writer writer) throws IOException {
        for(SolrDocument doc : docs) {
            beginDocWrite(writer);
            for(int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                String val = "";
                if (doc.containsKey(field)) {
                    val = StringEscapeUtils.escapeCsv(Utils.getUTF8String(doc.getFieldValue(field).toString()));
                }
                write(field, val, i == fields.size(), writer);
            }
            endDocWrite(writer);
        }
    }

    public abstract void exportHeaderData(long numDocs, String query, String fq, String coreName, Writer writer) throws IOException;

    public abstract void export(SolrDocumentList docs, List<String> fields, Writer writer) throws IOException;

    public abstract void writeEmptyResultSet(Writer writer) throws IOException;

    public void closeWriters(Writer writer, ServletOutputStream outputStream) {
        Utils.closeResource(writer);
        Utils.closeResource(outputStream);
    }
}
