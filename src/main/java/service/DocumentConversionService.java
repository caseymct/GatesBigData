package service;


import LucidWorksApp.utils.Utils;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;

public interface DocumentConversionService {

    public String getLocalTmpDirectory();

    public void writeLocalCopy(Content content, String hdfsFilePath, StringWriter writer) throws IOException;

    public String convertDocumentToSwf(String localFilePath);

    public String convertContentToThumbnail(Content content, Text url);

    public String getThumbnailNameFromHDFSPath(String hdfsPath);

    public String getSwfFileNameFromLocalFileName(String localFilePath);

    public String getSwfFileNameFromLocalFileName(File localFile);

    public File getSwfFile(String localFilePath);

    public File getSwfFile(File localFile);
}
