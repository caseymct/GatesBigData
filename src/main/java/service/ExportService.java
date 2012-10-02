package service;

import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;


public interface ExportService {

    static final String DEFAULT_DELIMETER = ",";
    static final String DEFAULT_NEWLINE = "\n";

    void exportHeaderData(long numDocs, String solrParams, final Writer writer);

    void exportHeaderData(long numDocs, String solrParams, final Writer writer, String delimiter, String newLine);

    void export(SolrDocumentList docs, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException;

    void export(SolrDocumentList docs, String coreName, final Writer writer, String delimeter, String newLine) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException;

    void writeEmptyResultSet(final Writer writer);

    void closeWriters(final Writer writer);
}
