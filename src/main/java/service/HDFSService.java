package service;


import net.sf.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public interface HDFSService {

    public void testCrawlData(String coreName, String segment);

    public Path getHDFSCoreDirectory(boolean includeURI, String coreName);

    public Path getHDFSCrawlDBCurrentDataFile(boolean includeURI, String coreName);

    public Path getHDFSCrawlDirectory(boolean includeURI, String coreName);

    public Path getHDFSSegmentsDirectory(boolean includeURI, String coreName);

    public Path getHDFSSegmentDirectory(boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlFetchDataFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlGenerateFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSContentDirectory(boolean includeURI, String coreName, String segment);

    public Path getHDFSContentDataFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSFacetFieldsCustomFile(String coreName);

    public TreeMap<String, String> getHDFSFacetFields(String hdfsDir);

    public Path getHDFSPreviewFieldsCustomFile(String coreName);

    public List<String> getHDFSPreviewFields(String coreName);

    public boolean addFile(String remoteFilePath, String localFilePath);

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory);

    public boolean removeFile(String remoteFilePath);

    public String getFileContents(Path remoteFilePath);

    public byte[] getFileContentsAsBytes(Path remoteFilePath);

    public Content getFileContents(String coreName, String segment, String fileName) throws IOException;

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException;

    public Content getContent(String coreName, String segment, String filePath, StringWriter writer) throws IOException;

    public HashMap<Text, Content> getAllContents(String coreName) throws IOException;

    public void generateThumbnails(String coreName) throws IOException;

    public Path getHDFSThumbnailPathFromHDFSDocPath(String coreName, String segment, String hdfsPath);

    public List<String> listFiles(Path hdfsDirectory, boolean recurse);

    public List<String> listSegments(String coreName);

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer);

    public void printFileContents(String coreName, String hdfsDate, String fileName, StringWriter writer, boolean preview);
}
