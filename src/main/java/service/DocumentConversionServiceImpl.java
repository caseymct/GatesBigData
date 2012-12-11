package service;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;

import org.apache.log4j.Logger;
import org.apache.nutch.protocol.Content;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

@Service
public class DocumentConversionServiceImpl implements DocumentConversionService {

    private static final String TEST_PRIZM_CONVERT_URL     = "http://localhost:18680/convert2swf";
    private static final String TEST_TMP_DIRECTORY         = "C:/tmp/";
    private static final String PROD_PRIZM_CONVERT_URL     = "http://denlx006.dn.gates.com:18880/convert2swf";
    private static final String PROD_TMP_DIRECTORY         = "/tmp/prizm/";
    private static String TMP_DIRECTORY = Utils.runningOnProduction() ? PROD_TMP_DIRECTORY : TEST_TMP_DIRECTORY;
    private static String CONVERT_URL = Utils.runningOnProduction() ? PROD_PRIZM_CONVERT_URL : TEST_PRIZM_CONVERT_URL;

    private static final String THUMBNAIL_SIZE = "5000x5000";

    private static final Logger logger = Logger.getLogger(DocumentConversionServiceImpl.class);

    public String getLocalTmpDirectory() {
        return TMP_DIRECTORY;
    }

    public void writeLocalCopy(byte[] content, String fileName, StringWriter writer) throws IOException {

        File tmpFile = new File(TMP_DIRECTORY, fileName);
        if (tmpFile.exists() && !tmpFile.delete()) {
            Utils.printFileErrorMessage(writer, "Can not delete old file " + tmpFile.getPath());
            return;
        }

        boolean success = tmpFile.createNewFile();
        if (!success || !tmpFile.canWrite()) {
            Utils.printFileErrorMessage(writer, "Can not write file " + tmpFile.getPath());
            return;
        }

        if (Utils.writeLocalFile(tmpFile, content)) {
            writer.append(tmpFile.getPath());
        }
    }

    public String getSwfFileNameFromLocalFileName(String localFilePath) {
        return getSwfFileNameFromLocalFileName(new File(localFilePath));
    }

    public String getSwfFileNameFromLocalFileName(File localFile) {
        return Utils.changeFileExtension(localFile.getName(), Constants.SWF_FILE_EXT, false);
    }

    public File getSwfFile(String localFilePath) {
        String swfFileName = getSwfFileNameFromLocalFileName(new File(localFilePath));
        return new File(TMP_DIRECTORY, swfFileName);
    }

    public File getSwfFile(File localFile) {
        String swfFileName = getSwfFileNameFromLocalFileName(localFile);
        return new File(TMP_DIRECTORY, swfFileName);
    }

    public String convertDocumentToSwf(String localFilePath) {

        File localFile = new File(localFilePath);
        String swfFileName = getSwfFileNameFromLocalFileName(localFile);

        HashMap<String, String> params = new HashMap<String, String>();
        params.put("source", TMP_DIRECTORY + localFile.getName());
        params.put("target", TMP_DIRECTORY + swfFileName);

        HttpClientUtils.httpGetRequest(CONVERT_URL + Utils.constructUrlParams(params));
        cleanupTempDir(localFile);

        File localSwfFile = getSwfFile(localFile);
        return localSwfFile.exists() ? localSwfFile.getPath() : Utils.getFileErrorString();
    }

    public String getTempDocNameFromHDFSId(String hdfsPath) {
        hdfsPath = Utils.decodeUrl(hdfsPath);
        return hdfsPath.replaceAll("^.*:\\/\\/", "").replaceAll("/", "_").replaceAll(" |\\$|~", "");
    }

    public String getThumbnailNameFromHDFSPath(String hdfsPath) {
        return Utils.changeFileExtension(getTempDocNameFromHDFSId(hdfsPath), Constants.IMG_FILE_EXT, false);
    }

    public String convertContentToThumbnail(Content content, String url) {
        String uri = Utils.decodeUrl(url);
        if (uri.endsWith("/")) {
            return "";
        }

        String fileName = getTempDocNameFromHDFSId(uri);
        String imgFileName = getThumbnailNameFromHDFSPath(uri);

        File localFile = new File(TMP_DIRECTORY, fileName);

        try {
            if (Utils.writeLocalFile(localFile, content.getContent())) {

                HashMap<String, String> params = new HashMap<String, String>();
                params.put("source", TMP_DIRECTORY + fileName);
                params.put("target", TMP_DIRECTORY + imgFileName);
                //params.put("thumbnail", THUMBNAIL_SIZE);
                params.put("pages", "1");

                HttpClientUtils.httpGetRequest(CONVERT_URL + Utils.constructUrlParams(params));

                cleanupTempDir(localFile);

                File localThumbnail = new File(TMP_DIRECTORY, imgFileName);
                if (localThumbnail.exists()) {
                    return localThumbnail.getPath();
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return Utils.getFileErrorString();
    }

    private void cleanupTempDir(File localFile) {
        String pdfFileName = Utils.changeFileExtension(localFile.getName(), Constants.PDF_FILE_EXT, false);
        Utils.removeLocalFile(localFile);
        Utils.removeLocalFile(new File(TMP_DIRECTORY, pdfFileName));
    }

}
