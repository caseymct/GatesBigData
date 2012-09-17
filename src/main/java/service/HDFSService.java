package service;


import net.sf.json.JSONObject;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerator;

import java.util.List;
import java.util.TreeMap;

public interface HDFSService {

    public void testCrawlData(String segment);

    public Path getHDFSSegmentsDirectory(boolean includeURI);

    public Path getHDFSSegmentDirectory(String segment, boolean includeURI);

    public Path getHDFSCrawlDirectory(boolean includeURI);

    public Path getHDFSContentDirectory(String segment, boolean includeURI);

    public Path getHDFSContentDataFile(String segment, boolean includeURI);

    public Path getHDFSCoreDirectory(String coreName);

    public Path getHDFSFacetFieldCustomFile(String coreName);

    public Path getHDFSCrawlFetchDataFile(String segment, boolean includeURI);

    public Path getHDFSCrawlGenerateFile(String segment, boolean includeURI);

    public TreeMap<String, String> getHDFSFacetFields(String hdfsDir);

    public boolean addFile(String remoteFilePath, String localFilePath);

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory);

    public boolean removeFile(String remoteFilePath);

    public JSONObject getJSONFileContents(Path remoteFilePath);

    public String getFileContents(Path remoteFilePath);

    public List<String> listFiles(Path hdfsDirectory, boolean recurse);

    public List<String> listSegments();

    /* Returns a map of files to their segment */
    public TreeMap<String, String> listFilesInCrawlDirectory();

    public void printFileContents(String hdfsDate, String fileName, JsonGenerator g);
}
