package service;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.SolrUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;

public class ExportCSVServiceImpl extends ExportService {

    private static final Logger logger = Logger.getLogger(ExportCSVServiceImpl.class);
    private HDFSService hdfsService;

    @Autowired
    public void setServices(HDFSService hdfsService) {
        this.hdfsService = hdfsService;
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer, String delimiter, String newLine) {
        query = "\"" + query.replaceAll("\"", "\"\"") + "\"";
        fq = (fq == null) ? "" : "\"" + fq.replaceAll("\"", "\"\"") + "\"";

        try {
            writer.append("# Found").append(delimiter).append("Solr Query").append(delimiter);
            writer.append("Core name").append(delimiter).append("Filter Query").append(delimiter);
            writer.append(newLine);

            writer.append(Long.toString(numDocs)).append(delimiter).append(query).append(delimiter);
            writer.append(coreName).append(delimiter).append(fq).append(delimiter);
            writer.append(newLine).append(newLine);

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

    private void buildCSVHeaderRow(List<String> fields, String newLine, String delimeter, final Writer writer) throws IOException {
        writer.append(StringUtils.join(fields, delimeter));
        writer.append(newLine);
    }


    public void exportJSONDocs(JSONArray docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        logger.debug("Exporting JSON docs to CSV");

        writer.append("Documents with Content-type : ").append(Constants.JSON_CONTENT_TYPE).append(newLine);

        // build the csv header row
        if (fields.size() == 0) {
            fields = SolrUtils.getLukeFieldNames(coreName);
        }
        buildCSVHeaderRow(fields, newLine, delimeter, writer);

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

    public void export(JSONArray docs, List<String> fields, String coreName, final Writer writer, String delimeter, String newLine) throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        logger.debug("Exporting to CSV");

        // build the csv header row
        fields = FIELDS_TO_EXPORT;
        buildCSVHeaderRow(fields, newLine, delimeter, writer);

        for(int i = 0; i < docs.size(); i++) {
            JSONObject doc = docs.getJSONObject(i);

            Iterator<String> fieldIterator = fields.iterator();
            while(fieldIterator.hasNext()) {
                String field = fieldIterator.next();
                Object val = doc.getString(field);
                writer.append(StringEscapeUtils.escapeCsv(val != null ? new String(val.toString().getBytes(), Charset.forName(Constants.UTF8)) : ""));
                if (fieldIterator.hasNext()) {
                    writer.append(delimeter);
                }
            }
            writer.append(newLine);
        }
        writer.flush();
    }
}
