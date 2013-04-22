package service;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import model.InMemoryOutputStream;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import javax.servlet.ServletOutputStream;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExportZipServiceImpl extends ExportService {

    private ZipOutputStream zipOutputStream;
    private InMemoryOutputStream inMemoryOutputStream;

    private static String HEADER_FILENAME = "README.txt";
    private static final Logger logger = Logger.getLogger(ExportZipServiceImpl.class);
    private StringBuilder headerStringBuilder = new StringBuilder();

    public void writeDocsWithNullContentToHeader(HashMap<String, String> docsWithNullContent) {
        if (!Utils.nullOrEmpty(docsWithNullContent)) {
            headerStringBuilder.append("Number of documents found with null content : ")
                    .append(docsWithNullContent.size()).append(Constants.DEFAULT_NEWLINE);

            for(Map.Entry<String, String> entry : docsWithNullContent.entrySet()) {
                headerStringBuilder.append("\t");
                headerStringBuilder.append(Constants.SOLR_FIELD_NAME_TITLE).append(" : ").append(entry.getValue());
                headerStringBuilder.append(Constants.DEFAULT_DELIMETER).append(" ");
                headerStringBuilder.append(Constants.SOLR_FIELD_NAME_ID).append(" : ").append(entry.getKey());
                headerStringBuilder.append(Constants.DEFAULT_NEWLINE);
            }
        }
    }

    public void export(SolrDocumentList docs, List<String> fields, Writer writer) throws IOException {

        HashMap<String, String> docsWithNullContent = new HashMap<String, String>();

        for(SolrDocument doc : docs) {
            String contentStr = SolrUtils.getFieldStringValue(doc, Constants.SOLR_FIELD_NAME_CONTENT, "");
            String title      = SolrUtils.getFieldStringValue(doc, Constants.SOLR_FIELD_NAME_TITLE, "No title");

            if (!Utils.nullOrEmpty(contentStr)) {
                writeToZipOutputStream(contentStr.getBytes(), title);
            } else {
                docsWithNullContent.put(SolrUtils.getFieldStringValue(doc, Constants.SOLR_FIELD_NAME_ID, ""), title);
            }
        }


        writeDocsWithNullContentToHeader(docsWithNullContent);
        writeHeaderFile();
    }

    public void beginExportWrite(Writer writer) throws IOException {
        inMemoryOutputStream = new InMemoryOutputStream();
        zipOutputStream = new ZipOutputStream(inMemoryOutputStream);
    }

    public void endExportWrite(Writer writer, ServletOutputStream outputStream) throws IOException {
        try {
            zipOutputStream.finish();
        } catch (ZipException e) {
            logger.error(e.getMessage());
        }

        writeContentsToZipFile();
        outputStream.write(getBytesFromZipOutputFile());

        Utils.closeResource(zipOutputStream);
        Utils.closeResource(inMemoryOutputStream);
        Utils.closeResource(outputStream);
        Utils.removeLocalFile(this.exportFileName);
    }

    private byte[] getBytesFromZipOutputFile() throws IOException {
        List<String> byteList = new ArrayList<String>();

        FileInputStream is = new FileInputStream(new File(this.exportFileName));
        int readLen = -1;
        byte[] buff = new byte[4096];

        while((readLen = is.read(buff)) != -1) {
            for (int i = 0; i < readLen; i++) {
                byteList.add(Byte.toString(buff[i]));
            }
        }
        Utils.closeResource(is);

        return getByteArrayFromList(byteList);
    }

    private byte[] getByteArrayFromList(List byteList) {
        byte[] buff = new byte[byteList.size()];

        for (int i = 0; i < byteList.size(); i++) {
            buff[i] = Byte.parseByte((String)byteList.get(i));
        }

        return buff;
    }

    private void writeHeaderFile() {
        writeToZipOutputStream(headerStringBuilder.toString().getBytes(), HEADER_FILENAME);
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, Writer writer) throws IOException {
        headerStringBuilder.append(NUM_FOUND_HDR).append(": ").append(Long.toString(numDocs)).append(Constants.DEFAULT_NEWLINE);
        headerStringBuilder.append(SOLR_QUERY_HDR).append(": ").append(query).append(Constants.DEFAULT_NEWLINE);
        headerStringBuilder.append(CORE_NAME_HDR).append(": ").append(coreName).append(Constants.DEFAULT_NEWLINE);
        headerStringBuilder.append(FILTER_QUERY_HDR).append(": ").append(fq).append(Constants.DEFAULT_NEWLINE);
    }

    private void writeToZipOutputStream(String inputString, String fileName) {
        writeToZipOutputStream(inputString.getBytes(), fileName);
    }

    private void writeToZipOutputStream(byte[] bytesToWrite, String fileName) {
        try {
            ZipParameters parameters = new ZipParameters();
            parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
            parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
            parameters.setFileNameInZip(fileName);
            parameters.setSourceExternalStream(true);

            zipOutputStream.putNextEntry(null, parameters);
            zipOutputStream.write(bytesToWrite);
            zipOutputStream.closeEntry();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeContentsToZipFile() {
        byte[] zipContent = inMemoryOutputStream.getZipContent();

        try {
            FileOutputStream os = new FileOutputStream(new File(this.exportFileName));
            os.write(zipContent);
            Utils.closeResource(os);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void writeEmptyResultSet(final Writer writer) throws IOException {}
    public void beginDocWrite(final Writer writer) throws IOException {}
    public void endDocWrite(final Writer writer) throws IOException {}
    public void write(String field, String value, boolean lastField, Writer writer) throws IOException {}
    public void exportHeaderRow(List<String> fields, final Writer writer) throws IOException {}
}
