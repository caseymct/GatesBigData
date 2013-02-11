package service;


import org.apache.nutch.protocol.Content;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;

public interface DocumentConversionService {

    public String getLocalTmpDirectory();

    public void writeLocalCopy(byte[] content, String hdfsFilePath, StringWriter writer) throws IOException;

    public String convertDocumentToSwf(String localFilePath);

    public String convertContentToThumbnail(Content content, String url);

    public String getThumbnailNameFromHDFSPath(String hdfsPath);

    public String getSwfFileNameFromLocalFileName(String localFilePath);

    public String getSwfFileNameFromLocalFileName(File localFile);

    public File getSwfFile(String localFilePath);

    public File getSwfFile(File localFile);
}
