package service;


import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

public interface HDFSService {

    public void testCrawlData(String coreName, String segment);

    public FileSystem getHDFSFileSystem();

    public Configuration getHDFSConfiguration();

    public Configuration getNutchConfiguration();

    public boolean addFile(String remoteFilePath, String localFilePath);

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory);

    public HashMap<String, MapFile.Reader[]> getSegmentToMapFileReaderMap(String coreName, String methodName);

    public HashMap<String, Long> getFetched(String coreName);

    public void writeFetched(String coreName, StringWriter writer);

    public boolean removeFile(String remoteFilePath);

    public String getFileContents(Path remoteFilePath);

    public Content getFileContents(String coreName, String segment, String fileName);

    public ParseData getParseData(String coreName, String segment, String fileName) throws IOException;

    public String getParsedText(String coreName, String segment, String fileName) throws IOException;

    public byte[] getFileContentsAsBytes(Path remoteFilePath);

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException;

    public Content getContent(String coreName, String segment, String filePath, StringWriter writer) throws IOException;

    public <T> T getContents(String coreName, Path dataFile, Class<T> clazz, Class<T> returnClass) throws IOException;

    public List<String> listFiles(Path hdfsDirectory, boolean recurse, Pattern filter, FileSystem fs);

    public List<String> listSegments(String coreName);

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer, boolean includeContentAndParseInfo);
}
