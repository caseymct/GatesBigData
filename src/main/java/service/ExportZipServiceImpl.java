package service;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.SolrUtils;
import LucidWorksApp.utils.Utils;
import model.InMemoryOutputStream;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.apache.avro.generic.GenericData;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import javax.servlet.ServletOutputStream;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExportZipServiceImpl extends ExportService {

    private ZipOutputStream zipOutputStream;
    private InMemoryOutputStream inMemoryOutputStream;

    private static String HEADER_FILENAME = "HeaderData.txt";
    private static final Logger logger = Logger.getLogger(ExportZipServiceImpl.class);
    private List<String> headerStrings = new ArrayList<String>();
    private static byte[] nl = System.getProperty("line.separator").getBytes();

    public void export(SolrDocumentList docs, List<String> fields, Writer writer) throws IOException {

        List<String> docsWithNullContent = new ArrayList<String>();

        for(SolrDocument doc : docs) {
            String contentStr = SolrUtils.getFieldValue(doc, Constants.SOLR_CONTENT_FIELD_NAME, "");
            String title = SolrUtils.getFieldValue(doc, Constants.SOLR_TITLE_FIELD_NAME, "No title");

            if (!Utils.nullOrEmpty(contentStr)) {
                writeToZipOutputStream(contentStr.getBytes(), title);
            } else {
                docsWithNullContent.add(title);
            }
        }

        if (!Utils.nullOrEmpty(docsWithNullContent)) {
            headerStrings.add("# Documents found with null content : " + docsWithNullContent.size());
            for(String title : docsWithNullContent) {
                headerStrings.add("\t" + title);
            }
        }
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

    private List<Byte> addStringBytes(String s, List<Byte> byteList) {
        for(byte b : s.getBytes()) {
            byteList.add(b);
        }
        return byteList;
    }

    private void writeHeaderFile() {
        int hdrLen = 0;

        List<Byte> headerBytes = new ArrayList<Byte>();
        for(String headerString : headerStrings) {
            headerBytes = addStringBytes(headerString, headerBytes);
            headerBytes = addStringBytes(nl, headerBytes);
        }
        for(byte[] b: headerBytes) {
            hdrLen += b.length;
        }
        byte[] hdr = new byte[hdrLen];

        int currlen = 0;
        for(byte[] h : headerBytes) {
            System.arraycopy(h, 0, hdr, currlen, h.length);
            currlen += h.length;
        }

        writeToZipOutputStream(hdr, HEADER_FILENAME);
    }

    public void exportHeaderData(long numDocs, String query, String fq, String coreName, Writer writer) throws IOException {
        headerStrings.add(NUM_FOUND_HDR    + ": " + Long.toString(numDocs));
        headerStrings.add(SOLR_QUERY_HDR   + ": " + query);
        headerStrings.add(CORE_NAME_HDR    + ": " + coreName);
        headerStrings.add(FILTER_QUERY_HDR + ": " + fq);
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
}
