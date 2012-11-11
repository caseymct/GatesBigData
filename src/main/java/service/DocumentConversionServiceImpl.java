package service;

import LucidWorksApp.utils.HttpClientUtils;
import LucidWorksApp.utils.Utils;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@Service
public class DocumentConversionServiceImpl implements DocumentConversionService {

    private static final String PRISM_CONVERT_URL = "http://localhost:18680/convert2swf";
    private static final String LOCAL_TMP_DIRECTORY = "C:/tmp/";
    //private static final String PRISM_CONVERT_URL = "http://denlx006.dn.gates.com:18880/convert2swf";
    //private static final String LOCAL_TMP_DIRECTORY = "/tmp/prizm/";
    private static final String THUMBNAIL_SIZE = "5000x5000";
    private static final String SWF_EXTENSION = "swf";
    private static final String IMG_EXTENSION = "png";

    public String getLocalTmpDirectory() {
        return LOCAL_TMP_DIRECTORY;
    }

    public void writeLocalCopy(Content content, String hdfsFilePath, StringWriter writer) throws IOException {

        String fileName = new File(hdfsFilePath).getName();
        if (fileName.endsWith(".json")) {
            fileName = fileName.replace(".json", ".txt");
        }
        File tmpFile = new File(LOCAL_TMP_DIRECTORY, fileName);
        if (tmpFile.exists() && !tmpFile.delete()) {
            Utils.printFileErrorMessage(writer, "Can not delete old file " + tmpFile.getPath());
            return;
        }

        boolean success = tmpFile.createNewFile();
        if (!success || !tmpFile.canWrite()) {
            Utils.printFileErrorMessage(writer, "Can not write file " + tmpFile.getPath());
            return;
        }

        if (Utils.writeLocalFile(tmpFile, content.getContent())) {
            writer.append(tmpFile.getPath());
        }
    }

    public String getSwfFileNameFromLocalFileName(String localFilePath) {
        return getSwfFileNameFromLocalFileName(new File(localFilePath));
    }

    public String getSwfFileNameFromLocalFileName(File localFile) {
        return Utils.changeFileExtension(localFile.getName(), SWF_EXTENSION, false);
    }

    public File getSwfFile(String localFilePath) {
        String swfFileName = getSwfFileNameFromLocalFileName(new File(localFilePath));
        return new File(LOCAL_TMP_DIRECTORY, swfFileName);
    }

    public File getSwfFile(File localFile) {
        String swfFileName = getSwfFileNameFromLocalFileName(localFile);
        return new File(LOCAL_TMP_DIRECTORY, swfFileName);
    }

    public String convertDocumentToSwf(String localFilePath) {

        File localFile = new File(localFilePath);
        String swfFileName = getSwfFileNameFromLocalFileName(localFile);

        HashMap<String, String> params = new HashMap<String, String>();
        params.put("source", LOCAL_TMP_DIRECTORY + localFile.getName());
        params.put("target", LOCAL_TMP_DIRECTORY + swfFileName);

        HttpClientUtils.httpGetRequest(PRISM_CONVERT_URL + Utils.constructUrlParams(params));
        //System.out.println(PRISM_CONVERT_URL + Utils.constructUrlParams(params));
        cleanupTempDir(localFile);

        File localSwfFile = getSwfFile(localFile);
        return localSwfFile.exists() ? localSwfFile.getPath() : Utils.getFileErrorString();
    }

    public String getTempDocNameFromHDFSId(String hdfsPath) {
        hdfsPath = Utils.decodeUrl(hdfsPath);
        return hdfsPath.replaceAll("^.*:\\/\\/", "").replaceAll("/", "_").replaceAll(" |\\$|~", "");
    }

    public String getThumbnailNameFromHDFSPath(String hdfsPath) {
        return Utils.changeFileExtension(getTempDocNameFromHDFSId(hdfsPath), IMG_EXTENSION, false);
    }

    public String convertContentToThumbnail(Content content, Text url) {
        String uri = Utils.decodeUrl(url.toString());
        if (uri.endsWith("/")) {
            return "";
        }

        String fileName = getTempDocNameFromHDFSId(uri);
        String imgFileName = getThumbnailNameFromHDFSPath(uri);

        File localFile = new File(LOCAL_TMP_DIRECTORY, fileName);

        try {
            if (Utils.writeLocalFile(localFile, content.getContent())) {

                HashMap<String, String> params = new HashMap<String, String>();
                params.put("source", LOCAL_TMP_DIRECTORY + fileName);
                params.put("target", LOCAL_TMP_DIRECTORY + imgFileName);
                //params.put("thumbnail", THUMBNAIL_SIZE);
                params.put("pages", "1");

                HttpClientUtils.httpGetRequest(PRISM_CONVERT_URL + Utils.constructUrlParams(params));

                cleanupTempDir(localFile);

                File localThumbnail = new File(LOCAL_TMP_DIRECTORY, imgFileName);
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
        String pdfFileName = Utils.changeFileExtension(localFile.getName(), "pdf", false);
        Utils.removeLocalFile(localFile);
        Utils.removeLocalFile(new File(LOCAL_TMP_DIRECTORY, pdfFileName));
    }

}
