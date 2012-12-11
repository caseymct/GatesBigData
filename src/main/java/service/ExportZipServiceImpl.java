package service;


import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.SolrUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONArray;
import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrDocumentList;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ExportZipServiceImpl extends ExportService {

    private FileOutputStream fileOutputStream;
    private ZipOutputStream zipOutputStream;
    private static String HEADER_FILENAME = "HeaderData.txt";

    private static final Logger logger = Logger.getLogger(ExportZipServiceImpl.class);
    private HDFSService hdfsService;

    @Autowired
    public void setServices(HDFSService hdfsService) {
        this.hdfsService = hdfsService;
    }

    public void initializeZipfile(String zipfileName) {
        try {
            fileOutputStream = new FileOutputStream(zipfileName);
            zipOutputStream = new ZipOutputStream(fileOutputStream);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        }
    }

    public void exportJSONDocs(JSONArray docs, List<String> fields, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        export(docs, fields, coreName, writer, Constants.DEFAULT_DELIMETER, Constants.DEFAULT_NEWLINE);
    }

    public void exportJSONDocs(JSONArray docs, List<String> fields, String coreName, final Writer writer, String delimiter, String newLine) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        export(docs, fields, coreName, writer, delimiter, newLine);
    }

    public void export(JSONArray docs, List<String> fields, String coreName, final Writer writer, String delimiter, String newLine) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        export(docs, fields, coreName, writer, delimiter, newLine);
    }

    public void export(JSONArray docs, List<String> fields, String coreName, final Writer writer) throws InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException {
        HashMap<String, List<String>> segToFileMap = SolrUtils.getSegmentToFilesMap(docs);

        for (Map.Entry<String, List<String>> entry : segToFileMap.entrySet()) {
            for (Content content : hdfsService.getFileContents(coreName, entry.getKey(), entry.getValue())) {

                if (content == null || !content.getContentType().equals(Constants.JSON_CONTENT_TYPE)) {
                    continue;
                }
                ByteArrayInputStream inputStream = new ByteArrayInputStream(content.getContent());
                DataInputStream in = new DataInputStream(inputStream);
                writeToZipOutputStream(in, content.getUrl());

                Utils.closeResource(in);
                Utils.closeResource(inputStream);
            }
        }
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer, String delimiter, String newLine) {
        initializeZipfile(this.exportFileName);
        StringBuilder inputStringBuilder = new StringBuilder();

        try {
            inputStringBuilder.append("# Found: ").append(Long.toString(numDocs));
            inputStringBuilder.append("Solr Query: ").append(query);
            inputStringBuilder.append("Core name: ").append(coreName);
            inputStringBuilder.append("Filter Query: ").append(fq);

            String inputString = inputStringBuilder.toString();
            ByteArrayInputStream stream = new ByteArrayInputStream(inputString.getBytes("UTF-8"));
            writeToZipOutputStream(stream, HEADER_FILENAME);

            Utils.closeResource(stream);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void writeToZipOutputStream(InputStream inputStream, String fileName) throws IOException {
        ZipEntry zipEntry = new ZipEntry(fileName);
        this.zipOutputStream.putNextEntry(zipEntry);

        byte[] bytes = new byte[1024];
        int length;
        while ((length = inputStream.read(bytes)) >= 0) {
            this.zipOutputStream.write(bytes, 0, length);
        }

        this.zipOutputStream.closeEntry();
    }

    public void addToZipFile(String fileName) throws IOException {

        System.out.println("Writing '" + fileName + "' to zip file");

        File file = new File(fileName);
        FileInputStream fis = new FileInputStream(file);
        writeToZipOutputStream(fis, fileName);
        fis.close();
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
}
