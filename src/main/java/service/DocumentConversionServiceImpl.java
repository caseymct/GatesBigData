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
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

@Service
public class DocumentConversionServiceImpl implements DocumentConversionService {

    private static final String PRISM_CONVERT_URL = "http://localhost:18680/convert2swf";
    private static final String LOCAL_TMP_DIRECTORY = "C:/tmp/";
    private static final String RELATIVE_TMP_DIRECTORY = "../../tmp/";

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

    public String convertDocumentToSwf(String localFilePath) {

        File localFile = new File(localFilePath);
        String swfFileName = Utils.changeFileExtension(localFile.getName(), SWF_EXTENSION, false);

        HashMap<String, String> params = new HashMap<String, String>();
        params.put("source", RELATIVE_TMP_DIRECTORY + localFile.getName());
        params.put("target", RELATIVE_TMP_DIRECTORY + swfFileName);

        HttpClientUtils.httpGetRequest(PRISM_CONVERT_URL + Utils.constructUrlParams(params));

        cleanupTempDir(localFile);

        File localSwfFile = new File(LOCAL_TMP_DIRECTORY, swfFileName);
        return localSwfFile.exists() ? localSwfFile.getPath() : Utils.getFileErrorString();
    }

    public String getTempDocNameFromHDFSId(String hdfsPath) {
        hdfsPath = decode(hdfsPath);
        return hdfsPath.replaceAll("^.*:\\/\\/", "").replaceAll("/", "_").replaceAll(" |\\$|~", "");
    }

    public String getThumbnailNameFromHDFSPath(String hdfsPath) {
        return Utils.changeFileExtension(getTempDocNameFromHDFSId(hdfsPath), IMG_EXTENSION, false);
    }

    public String decode(String url) {
        try {
            url = URLDecoder.decode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        }
        return url;
    }

    public String convertContentToThumbnail(Content content, Text url) {
        String uri = decode(url.toString());
        if (uri.endsWith("/")) {
            return "";
        }

        String fileName = getTempDocNameFromHDFSId(uri);
        String imgFileName = getThumbnailNameFromHDFSPath(uri);

        File localFile = new File(LOCAL_TMP_DIRECTORY, fileName);

        try {
            if (Utils.writeLocalFile(localFile, content.getContent())) {

                HashMap<String, String> params = new HashMap<String, String>();
                params.put("source", RELATIVE_TMP_DIRECTORY + fileName);
                params.put("target", RELATIVE_TMP_DIRECTORY + imgFileName);
                params.put("thumbnail", "1000x1000");
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

    public void test(HashMap<Text, Content> allContents) {
        for (Map.Entry<Text, Content> entry : allContents.entrySet()) {
            convertContentToThumbnail(entry.getValue(), entry.getKey());
        }
    }

}
