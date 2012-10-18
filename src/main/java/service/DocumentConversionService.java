package service;


import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;

public interface DocumentConversionService {

    public String getLocalTmpDirectory();

    public void writeLocalCopy(Content content, String hdfsFilePath, StringWriter writer) throws IOException;

    public String convertDocumentToSwf(String localFilePath);

    public String convertContentToThumbnail(Content content, Text url);

    public void test(HashMap<Text, Content> allContents);

    public String getThumbnailNameFromHDFSPath(String hdfsPath);
}
