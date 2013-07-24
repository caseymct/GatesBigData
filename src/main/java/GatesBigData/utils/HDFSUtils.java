package GatesBigData.utils;

import GatesBigData.constants.Constants;
import GatesBigData.constants.HDFS;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.log4j.Logger;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;


public class HDFSUtils {

    private static final Logger logger = Logger.getLogger(HDFSUtils.class);

    private static final String FILE_DOES_NOT_EXIST_METADATA_KEY = "FILE_DOES_NOT_EXIST";
    private static final String HDFS_ERROR_STRING = "ERROR";

    private static final Partitioner PARTITIONER = new HashPartitioner();

    public static boolean hasHDFSErrorString(String s) {
        return s.startsWith(HDFS_ERROR_STRING);
    }

    public static URI getHDFSURI() {
        return URI.create(HDFS.HDFS_URI_STRING);
    }

    public static Pattern getDataDirPattern(String nutchTypeDirName, boolean isData) {
        String endFileName = isData ? HDFS.DATA_DIR : HDFS.INDEX_DIR;
        return Pattern.compile(".*" + nutchTypeDirName + "\\/part-[0-9]+\\/" + endFileName + "$");
    }

    public static Pattern getCrawlDbCurrentIndexFilesPattern() {
        return getDataDirPattern(HDFS.CRAWLDB_DIR + "\\/" + CrawlDb.CURRENT_NAME, false);
    }

    public static Pattern getContentDataDirPattern() {
        return getDataDirPattern(Content.DIR_NAME, true);
    }

    public static Pattern getCrawlFetchDataDirPattern() {
        return getDataDirPattern(CrawlDatum.FETCH_DIR_NAME, true);
    }

    public static Pattern getParseDataDataDirPattern() {
        return getDataDirPattern(ParseData.DIR_NAME, true);
    }

    public static Pattern getParseTextDataDirPattern() {
        return getDataDirPattern(ParseText.DIR_NAME, true);
    }

    public static Path getHDFSUserDirectory(Boolean includeURI) {
        return new Path(includeURI ? HDFS.USER_DIR_URI : HDFS.USER_DIR);
    }

    public static Path getHDFSCoreDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSUserDirectory(includeURI), coreName);
    }

    public static Path getHDFSCrawlDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSCoreDirectory(includeURI, coreName), HDFS.CRAWL_DIR);
    }

    public static Path getHDFSSegmentsDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), HDFS.SEGMENTS_DIR);
    }

    public static Path getHDFSSegmentDirectory(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentsDirectory(includeURI, coreName), segment);
    }

    public static Path getHDFSCrawlFetchDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CrawlDatum.FETCH_DIR_NAME);
    }

    public static Path getHDFSContentDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), Content.DIR_NAME);
    }

    public static Path getHDFSParseDataDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), ParseData.DIR_NAME);
    }

    public static Path getHDFSParseTextDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), ParseText.DIR_NAME);
    }

    public static Path getHDFSCrawlDBDir(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), HDFS.CRAWLDB_DIR);
    }

    public static Path getHDFSCrawlDBCurrentDir(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDBDir(includeURI, coreName), CrawlDb.CURRENT_NAME);
    }

    public static Content fileDoesNotExistContent() {
        Content content = new Content();
        Metadata metadata = new Metadata();
        metadata.add(FILE_DOES_NOT_EXIST_METADATA_KEY, "true");
        content.setMetadata(metadata);
        return content;
    }

    public static boolean contentIndicatesFileDoesNotExist(Content content) {
        return content.getMetadata().get(FILE_DOES_NOT_EXIST_METADATA_KEY) != null;
    }

    public static String getParsedText(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException {
        if (!map.containsKey(segment)) {
            return "";
        }

        Text key = new Text(fileName);
        ParseText parseText = new ParseText();
        MapFileOutputFormat.getEntry(map.get(segment), PARTITIONER, key, parseText);
        String text = parseText.getText();

        return Utils.nullOrEmpty(text) ? null : Utils.getUTF8String(text);
    }

    public static ParseData getParseData(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException {
        if (!map.containsKey(segment)) {
            return null;
        }

        Text key = new Text(fileName);
        ParseData parseData = new ParseData();
        MapFileOutputFormat.getEntry(map.get(segment), PARTITIONER, key, parseData);

        return parseData;
    }

    public static Content getFileContents(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException {
        if (!map.containsKey(segment)) {
            return null;
        }

        Text key = new Text(fileName);
        Content content = new Content();
        MapFileOutputFormat.getEntry(map.get(segment), PARTITIONER, key, content);

        return content;
    }

    public static List<Content> getContentList(List<String> fileNames, MapFile.Reader[] readers) throws IOException {
        List<Content> contents = new ArrayList<Content>();

        for(String fileName : fileNames) {
            Text key = new Text(fileName);
            Content content = new Content();
            MapFileOutputFormat.getEntry(readers, PARTITIONER, key, content);
            contents.add(content);
        }

        return contents;
    }

    public static String getContentTypeFromParseData(ParseData parseData){
        String contentType = parseData.getContentMeta().get(HttpHeaders.CONTENT_TYPE);
        if (Utils.nullOrEmpty(contentType)) {
            contentType = parseData.getParseMeta().get(HttpHeaders.CONTENT_TYPE);
        }
        return (contentType == null) ? "" : contentType;
    }
}
