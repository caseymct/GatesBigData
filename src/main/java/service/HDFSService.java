package service;


import model.FacetFieldEntryList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

public interface HDFSService {

    public void testCrawlData(String coreName, String segment);

    public Path getHDFSCoreDirectory(boolean includeURI, String coreName);

    public Path getHDFSCrawlDBCurrentDataFile(boolean includeURI, String coreName);

    public Path getHDFSCrawlDBCurrentIndexFile(boolean includeURI, String coreName);

    public Path getHDFSCrawlDirectory(boolean includeURI, String coreName);

    public Path getHDFSSegmentsDirectory(boolean includeURI, String coreName);

    public Path getHDFSSegmentDirectory(boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlFetchDataFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSCrawlGenerateFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSContentDirectory(boolean includeURI, String coreName, String segment);

    public Path getHDFSContentDataFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSParseDataFile(boolean includeURI, String coreName, String segment);

    public Path getHDFSFacetFieldsCustomFile(String coreName);

    public Configuration getHDFSConfiguration();

    public Configuration getNutchConfiguration();

    public FileSystem getHDFSFileSystem();

    public FacetFieldEntryList getHDFSFacetFields(String hdfsDir);

    public Path getHDFSPreviewFieldsCustomFile(String coreName);

    public List<String> getHDFSPreviewFields(String coreName);

    public boolean addFile(String remoteFilePath, String localFilePath);

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory);

    public long getFetched(String coreName);

    public boolean removeFile(String remoteFilePath);

    public String getFileContents(Path remoteFilePath);

    public byte[] getFileContentsAsBytes(Path remoteFilePath);

    public Content getFileContents(String coreName, String segment, String fileName) throws IOException;

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
