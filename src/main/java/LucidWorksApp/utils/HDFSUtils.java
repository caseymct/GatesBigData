package LucidWorksApp.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
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
import org.apache.nutch.util.NutchConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class HDFSUtils {

    private static final String FACETFIELDS_HDFSFILENAME    = "facetfields.csv";
    private static final String VIEWFIELDS_HDFSFILENAME     = "fields.csv";
    private static final String PREVIEWFIELDS_HDFSFILENAME  = "previewfields.csv";
    private static final String HDFS_USERNAME               = "hdfs";
    private static final String HDFS_URI                    = "hdfs://denlx006.dn.gates.com:8020";
    //private static final String HDFS_URI = "hdfs://127.0.0.1:8020";

    private static final String USER_HDFS_DIR   = "user/hdfs";
    private static final String CRAWL_DIR       = "crawl";
    private static final String SEGMENTS_DIR    = "segments";
    private static final String CRAWLDB_DIR     = "crawldb";
    private static final String PART_00000      = "part-00000";
    private static final String DATA_DIR        = "data";
    private static final String INDEX_DIR       = "index";

    private static final Path PART_DATA             = new Path(PART_00000, DATA_DIR);
    private static final Path PART_INDEX            = new Path(PART_00000, INDEX_DIR);
    private static final Path CONTENT_DATA          = new Path(Content.DIR_NAME, PART_DATA);
    private static final Path PARSE_DATA_DIR        = new Path(ParseData.DIR_NAME);
    private static final Path PARSE_DATA_FILE       = new Path(PARSE_DATA_DIR, PART_DATA);
    private static final Path PARSE_TEXT_DIR        = new Path(ParseText.DIR_NAME);
    private static final Path PARSE_TEXT_DATA_FILE  = new Path(PARSE_TEXT_DIR, PART_DATA);
    private static final Path CRAWL_PARSE_DIR       = new Path(CrawlDatum.PARSE_DIR_NAME);
    private static final Path CRAWL_FETCH_DATA      = new Path(CrawlDatum.FETCH_DIR_NAME, PART_DATA);
    private static final Path CRAWL_GENERATE_DATA   = new Path(CrawlDatum.GENERATE_DIR_NAME, PART_00000);
    private static final Path CRAWL_PARSE_DATA      = new Path(CRAWL_PARSE_DIR, PART_00000);

    private static final Logger logger = Logger.getLogger(HDFSUtils.class);

    private static final String FILE_DOES_NOT_EXIST_METADATA_KEY = "FILE_DOES_NOT_EXIST";
    private static final String HDFS_ERROR_STRING = "ERROR";

    private static final Partitioner PARTITIONER = new HashPartitioner();

    public static boolean hasHDFSErrorString(String s) {
        return s.startsWith(HDFS_ERROR_STRING);
    }

    public static Partitioner getPartitioner() {
        return PARTITIONER;
    }

    public static String getHdfsUri() {
        return HDFS_URI;
    }

    public static String getHdfsUsername() {
        return HDFS_USERNAME;
    }

    public static Path getHDFSCoreDirectory(Boolean includeURI, String coreName) {
        return new Path((includeURI ? HDFS_URI : "") + "/" + USER_HDFS_DIR, coreName);
    }

    public static Path getHDFSCrawlDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSCoreDirectory(includeURI, coreName), CRAWL_DIR);
    }

    public static Path getHDFSSegmentsDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), SEGMENTS_DIR);
    }

    public static Path getHDFSSegmentDirectory(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentsDirectory(includeURI, coreName), segment);
    }

    public static Path getHDFSCrawlFetchDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_FETCH_DATA);
    }

    public static Path getHDFSCrawlFetchDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CrawlDatum.FETCH_DIR_NAME);
    }


    public static Path getHDFSCrawlGenerateFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_GENERATE_DATA);
    }

    public static Path getHDFSContentDirectory(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), Content.DIR_NAME);
    }

    public static Path getHDFSContentDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CONTENT_DATA);
    }

    public static Path getHDFSParseDataDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_DATA_DIR);
    }

    public static Path getHDFSParseDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_DATA_FILE);
    }

    public static Path getHDFSParseTextDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_TEXT_DIR);
    }

    public static Path getHDFSParseTextDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_TEXT_DATA_FILE);
    }

    public static Path getHDFSCrawlParseDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_PARSE_DIR);
    }

    public static Path getHDFSCrawlParseDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_PARSE_DATA);
    }

    public static Path getHDFSCrawlDBDir(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), CRAWLDB_DIR);
    }

    public static Path getHDFSCrawlDBCurrentDir(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDBDir(includeURI, coreName), CrawlDb.CURRENT_NAME);
    }

    public static Path getHDFSCrawlDBCurrentDataFile(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDBCurrentDir(includeURI, coreName), PART_DATA);
    }

    public static Path getHDFSCrawlDBCurrentIndexFile(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDBCurrentDir(includeURI, coreName), PART_INDEX);
    }

    public static Path getHDFSFacetFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), FACETFIELDS_HDFSFILENAME);
    }

    public static Path getHDFSViewFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), VIEWFIELDS_HDFSFILENAME);
    }

    public static Path getHDFSPreviewFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), PREVIEWFIELDS_HDFSFILENAME);
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

        return Utils.getUTF8String(parseText.getText());
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
        if (Utils.stringIsNullOrEmpty(contentType)) {
            contentType = parseData.getParseMeta().get(HttpHeaders.CONTENT_TYPE);
        }
        return (contentType == null) ? "" : contentType;
    }
}
