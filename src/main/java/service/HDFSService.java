package service;


import model.FacetFieldEntryList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;

public interface HDFSService {

    public void testCrawlData(String coreName, String segment);

    public Path getHDFSCoreDirectory(Boolean includeURI, String coreName);

    public Path getHDFSCrawlDBCurrentDataFile(Boolean includeURI, String coreName);

    public Path getHDFSCrawlDBCurrentIndexFile(Boolean includeURI, String coreName);

    public Path getHDFSCrawlDirectory(Boolean includeURI, String coreName);

    public Path getHDFSSegmentsDirectory(Boolean includeURI, String coreName);

    public Path getHDFSSegmentDirectory(Boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlFetchDataFile(Boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlGenerateFile(Boolean includeURI, String coreName, String segment);

    public Path getHDFSParseDataDir(Boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlParseDataFile(Boolean includeURI, String coreName, String segment);

    public Path getHDFSContentDirectory(Boolean includeURI, String coreName, String segment);

    public Path getHDFSContentDataFile(Boolean includeURI, String coreName, String segment);

    public Path getHDFSParseDataFile(Boolean includeURI, String coreName, String segment);

    public Path getHDFSParseTextDir(Boolean includeURI, String coreName, String segment);

    public Path getHDFSParseTextDataFile(Boolean includeURI, String coreName, String segment);

    public Path getHDFSFacetFieldsCustomFile(String coreName);

    public Configuration getHDFSConfiguration();

    public Configuration getNutchConfiguration();

    public FileSystem getHDFSFileSystem();

    public FacetFieldEntryList getHDFSFacetFields(String hdfsDir);

    public Path getHDFSPreviewFieldsCustomFile(String coreName);

    public List<String> getHDFSPreviewFields(String coreName);

    public boolean addFile(String remoteFilePath, String localFilePath);

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory);

    public HashMap<String, MapFile.Reader[]> getSegmentToMapFileReaderMap(String coreName, String methodName);

    public long getFetched(String coreName);

    public boolean removeFile(String remoteFilePath);

    public String getFileContents(Path remoteFilePath);

    public Content getFileContents(String coreName, String segment, String fileName) throws IOException;

    public Content getFileContents(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException;

    public ParseData getParseData(String coreName, String segment, String fileName) throws IOException;

    public ParseData getParseData(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException;

    public String getParsedText(String coreName, String segment, String fileName) throws IOException;

    public String getParsedText(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException;

    public byte[] getFileContentsAsBytes(Path remoteFilePath);

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException;

    public Content getContent(String coreName, String segment, String filePath, StringWriter writer) throws IOException;

    public <T> T getContents(String coreName, Path dataFile, Class<T> clazz, Class<T> returnClass) throws IOException;

    public void generateThumbnails(String coreName) throws IOException;

    public Path getHDFSThumbnailPathFromHDFSDocPath(String coreName, String segment, String hdfsPath);

    public List<String> listFiles(Path hdfsDirectory, boolean recurse);

    public List<String> listSegments(String coreName);

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer);

    public void printFileContents(String coreName, String hdfsDate, String fileName, StringWriter writer, boolean preview);

    public String getContentTypeFromParseData(ParseData parseData);
}
