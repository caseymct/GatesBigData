package service;

import LucidWorksApp.utils.Constants;
import net.sf.json.JSONArray;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public abstract class ExportService {

    public String exportFileName            = "";
    public static List<String> FIELDS_TO_EXPORT = new ArrayList<String>(Arrays.asList("title", "id", "content_type", "preview"));

    public void setExportFileName(String exportFileName) {
        this.exportFileName = exportFileName;
    }

    public String getExportFileName() {
        return this.exportFileName;
    }

    public void writeDefaultNewline(final Writer writer) throws IOException {
        writer.append(Constants.DEFAULT_NEWLINE);
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer) {
        exportHeaderData(numDocs, query, fq, coreName, writer, Constants.DEFAULT_DELIMETER, Constants.DEFAULT_NEWLINE);
    }

    public void export(JSONArray docs, List<String> fields, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        export(docs, fields, coreName, writer, Constants.DEFAULT_DELIMETER, Constants.DEFAULT_NEWLINE);
    }

    public void exportJSONDocs(JSONArray docs, List<String> fields, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        exportJSONDocs(docs, fields, coreName, writer, Constants.DEFAULT_DELIMETER, Constants.DEFAULT_NEWLINE);
    }

    public abstract void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer, String delimiter, String newLine);

    public abstract void exportJSONDocs(JSONArray docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException;

    public abstract void export(JSONArray docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException;

    public abstract void writeEmptyResultSet(final Writer writer);

    public abstract void closeWriters(final Writer writer);
}
