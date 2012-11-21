package service;

import LucidWorksApp.utils.Constants;
import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import model.FacetFieldEntryList;
import model.HDFSNutchCoreFileIterator;
import model.NotifyingThread;
import model.ThreadCompleteListener;
import net.sf.json.JSONObject;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.log4j.Logger;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;

@Service
public class HDFSServiceImpl implements HDFSService, ThreadCompleteListener {

    private DocumentConversionService documentConversionService;

    private static final String FACETFIELDS_HDFSFILENAME    = "fields.csv";
    private static final String PREVIEWFIELDS_HDFSFILENAME  = "previewfields.csv";
    private static final String HDFS_URI                    = "hdfs://denlx006.dn.gates.com:8020";
    //private static final String HDFS_URI = "hdfs://127.0.0.1:8020";

    private static final String USER_HDFS_DIR   = "user/hdfs";
    private static final String CRAWL_DIR       = "crawl";
    private static final String SEGMENTS_DIR    = "segments";
    private static final String CRAWLDB_CURRENT = "crawldb/current";
    private static final String PART_00000      = "part-00000";
    private static final String DATA_DIR        = "data";
    private static final String INDEX_DIR       = "index";
    private static final String THUMBNAILS_DIR  = "thumbnails";

    private static final Path PART_DATA             = new Path(PART_00000, DATA_DIR);
    private static final Path PART_INDEX            = new Path(PART_00000, INDEX_DIR);
    private static final Path CONTENT_DATA          = new Path(Content.DIR_NAME, PART_DATA);
    private static final Path PARSE_DATA_DIR        = new Path(ParseData.DIR_NAME);
    private static final Path PARSE_DATA_FILE       = new Path(PARSE_DATA_DIR, PART_DATA);
    private static final Path PARSE_TEXT_DIR        = new Path(ParseText.DIR_NAME);
    private static final Path PARSE_TEXT_DATA_FILE  = new Path(PARSE_TEXT_DIR, PART_DATA);
    private static final Path CRAWL_FETCH_DATA      = new Path(CrawlDatum.FETCH_DIR_NAME, PART_DATA);
    private static final Path CRAWL_GENERATE_DATA   = new Path(CrawlDatum.GENERATE_DIR_NAME, PART_00000);
    private static final Path CRAWL_PARSE_DATA      = new Path(CrawlDatum.PARSE_DIR_NAME, PART_00000);

    private static final String FILE_DOES_NOT_EXIST_METADATA_KEY = "FILE_DOES_NOT_EXIST";
    private static final String HDFS_ERROR_STRING = "ERROR";

    private static final int N_THUMBNAIL_THREADS = 10;
    private List<Thread> thumbnailThreads = new ArrayList<Thread>();

    private static final Partitioner PARTITIONER = new HashPartitioner();
    private Configuration nutchConf     = getNutchConfiguration();
    private Configuration hdfsConf      = getHDFSConfiguration();
    private FileSystem hdfsFileSystem   = getHDFSFileSystem();

    private static final Logger logger = Logger.getLogger(HDFSServiceImpl.class);

    @Autowired
    public void setServices(DocumentConversionService documentConversionService) {
        this.documentConversionService = documentConversionService;
    }

    public Path getHDFSCoreDirectory(Boolean includeURI, String coreName) {
        return new Path((includeURI ? HDFS_URI : "") + "/" + USER_HDFS_DIR, coreName);
    }

    public Path getHDFSCrawlDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSCoreDirectory(includeURI, coreName), CRAWL_DIR);
    }

    public Path getHDFSSegmentsDirectory(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), SEGMENTS_DIR);
    }

    public Path getHDFSSegmentDirectory(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentsDirectory(includeURI, coreName), segment);
    }

    public Path getHDFSCrawlFetchDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_FETCH_DATA);
    }

    public Path getHDFSCrawlGenerateFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_GENERATE_DATA);
    }

    public Path getHDFSContentDirectory(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), Content.DIR_NAME);
    }

    public Path getHDFSContentDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CONTENT_DATA);
    }

    public Path getHDFSParseDataDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_DATA_DIR);
    }

    public Path getHDFSParseDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_DATA_FILE);
    }

    public Path getHDFSParseTextDir(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_TEXT_DIR);
    }

    public Path getHDFSParseTextDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), PARSE_TEXT_DATA_FILE);
    }

    public Path getHDFSCrawlParseDataFile(Boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_PARSE_DATA);
    }

    public Path getHDFSCrawlDBCurrentDataFile(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), CRAWLDB_CURRENT + "/" + PART_DATA);
    }

    public Path getHDFSCrawlDBCurrentIndexFile(Boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), CRAWLDB_CURRENT + "/" + PART_INDEX);
    }

    public Path getHDFSFacetFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), FACETFIELDS_HDFSFILENAME);
    }

    public Path getHDFSPreviewFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), PREVIEWFIELDS_HDFSFILENAME);
    }

    public Configuration getHDFSConfiguration() {
        return new Configuration();
    }

    public Configuration getNutchConfiguration() {
        return NutchConfiguration.create();
    }

    public FileSystem getHDFSFileSystem() {
        try {
            return FileSystem.get(URI.create(HDFS_URI), getHDFSConfiguration(), "hdfs");
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory) {
        int nAdded = 0;

        try {
            //FileSystem fs = getHDFSFileSystem();
            File localFileDir = new File(localFileDirectory);
            File[] localFiles = localFileDir.listFiles();

            if (localFiles != null && localFiles.length > 0) {
                for(File file : localFiles) {

                    Path srcPath = new Path(file.getAbsolutePath());
                    Path dstPath = new Path(remoteFileDirectory + "/" + file.getName());

                    if (!hdfsFileSystem.exists(dstPath)) {
                        try {
                            hdfsFileSystem.copyFromLocalFile(srcPath, dstPath);
                            nAdded++;
                        } catch(Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                }
            }

            hdfsFileSystem.close();
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return nAdded;
    }

    public boolean addFile(String remoteFilePath, String localFilePath) {
        try {
            FileSystem fs = getHDFSFileSystem();
            Path srcPath = new Path(localFilePath);
            Path dstPath = new Path(remoteFilePath);

            if (fs.exists(dstPath)) {
                return false;
            }

            try {
                fs.copyFromLocalFile(srcPath, dstPath);
            } catch(Exception e) {
                System.err.println(e.getMessage());
                return false;
            } finally {
                fs.close();
            }
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean removeFile(String remoteFilePath) {
        try {
            FileSystem fs = getHDFSFileSystem();
            Path path = new Path(remoteFilePath);

            if (fs.exists(path)) {
                fs.delete(path, true);
                return true;
            }

            fs.close();
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return false;
    }

    public String getFileContents(Path remoteFilePath) {
        return new String(getFileContentsAsBytes(remoteFilePath));
    }

    public byte[] getFileContentsAsBytes(Path remoteFilePath) {
        try {
            //FileSystem fs = getHDFSFileSystem();
            if (!hdfsFileSystem.exists(remoteFilePath)) {
                logger.error("File " + remoteFilePath.toString() + " does not exist on HDFS. ");
                return new byte[0];
            }

            long length = hdfsFileSystem.getContentSummary(remoteFilePath).getLength();
            byte[] contents = new byte[(int)length];

            if (length > Integer.MAX_VALUE) {
                logger.error("File " + remoteFilePath.toString() + " is too large. ");
                return new byte[0];
            }

            DataInputStream d = new DataInputStream(hdfsFileSystem.open(remoteFilePath));
            int offset = 0, numRead = 0;
            while (offset < contents.length
                    && (numRead = d.read(contents, offset, contents.length-offset)) >= 0) {
                offset += numRead;
            }

            if (offset < contents.length) {
                throw new IOException("Could not completely read file " + remoteFilePath);
            }

            d.close();
            return contents;

        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return new byte[0];
    }

    public FacetFieldEntryList getHDFSFacetFields(String coreName) {
        FacetFieldEntryList facetFieldEntryList = new FacetFieldEntryList();

        String response = getFileContents(getHDFSFacetFieldsCustomFile(coreName));
        if (!response.startsWith(HDFS_ERROR_STRING)) {
            for(String ret : response.split(",")) {
                String[] n = ret.split(":");
                facetFieldEntryList.add(n[0], n[1], n[2]);
            }
        }

        return facetFieldEntryList;
    }

    public List<String> getHDFSPreviewFields(String coreName) {
        String response = getFileContents(getHDFSPreviewFieldsCustomFile(coreName));
        if (!response.startsWith(HDFS_ERROR_STRING)) {
            return Arrays.asList(response.split(","));
        }

        return null;
    }

    public List<String> listFiles(Path hdfsDirectory, boolean recurse) {
        List<String> filePaths = new ArrayList<String>();

        //FileSystem fs = getHDFSFileSystem();
        int hdfsUriStringLength = HDFS_URI.length();

        try {
            if (recurse) {
                RemoteIterator<LocatedFileStatus> r = hdfsFileSystem.listFiles(hdfsDirectory, true);
                while(r.hasNext()) {
                    String pathString = r.next().getPath().toString();
                    // just add the HDFS path: hdfs://denlx006.dn.gates.com/path/etc --> /path/etc
                    filePaths.add(pathString.substring(hdfsUriStringLength));
                }
            } else {
                for(FileStatus status : hdfsFileSystem.listStatus(hdfsDirectory)) {
                    filePaths.add(status.getPath().getName());
                }
            }
        } catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return filePaths;
    }

    public List<String> listSegments(String coreName) {
        return listFiles(getHDFSSegmentsDirectory(false, coreName), false);
    }

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer) {
        long total = 0;

        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);

            g.writeStartObject();

            for(String segment : listSegments(coreName)) {
                g.writeArrayFieldStart(segment);

                for(Map.Entry entry : getCrawlData(getHDFSCrawlFetchDataFile(true, coreName, segment)).entrySet()) {
                    g.writeString(entry.getKey().toString() + ", " + ((CrawlDatum) entry.getValue()).getStatus());
                    total++;
                }
                g.writeEndArray();
            }

            g.writeNumberField("Number of files: ", total);
            g.writeEndObject();
            g.close();

        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private TreeMap<Text, CrawlDatum> getCrawlData(Path path) throws IOException {
        TreeMap<Text, CrawlDatum> allContents = new TreeMap<Text, CrawlDatum>();
        SequenceFile.Reader reader= new SequenceFile.Reader(hdfsFileSystem, path, nutchConf);
        do {
            Text key = new Text();
            CrawlDatum value = new CrawlDatum();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while (true);

        return allContents;
    }

    public long getFetched(String coreName)  {
        Path path = getHDFSCrawlDBCurrentIndexFile(true, coreName);
        long fetched = 0L;

        try {
            SequenceFile.Reader reader= new SequenceFile.Reader(hdfsFileSystem, path, nutchConf);
            do {
                Text key = new Text();
                CrawlDatum value = new CrawlDatum();
                if (!reader.next(key, value)) break;
                if (value.getStatus() == 2) {
                    fetched++;
                }
            } while (true);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return fetched;
    }

    public <T> T getContents(String coreName, Path dataFile, Class<T> clazz, Class<T> returnClass) throws IOException {
        HashMap<Text, Object> contents = new HashMap<Text, Object>();

        //Path dataFile = getHDFSContentDataFile(true, coreName, segment);
        SequenceFile.Reader reader = new SequenceFile.Reader(hdfsFileSystem, dataFile, nutchConf);
        do {
            try {
                Text key = new Text();
                T value = clazz.newInstance();

                if (!reader.next(key, (Writable) value)) break;
                contents.put(key, value);
            } catch (InstantiationException e) {
                logger.error(e.getMessage());
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage());
            }
        } while(true);


        return returnClass.cast(contents);
    }

    private Path returnOrCreateDirectory(Path path, FileSystem fs) throws IOException {
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        return path;
    }

    public Path getHDFSThumbnailPathFromHDFSDocPath(String coreName, String segment, String hdfsPath) {
        String imgName = documentConversionService.getThumbnailNameFromHDFSPath(hdfsPath);
        Path thumbnailPath = new Path(getHDFSCoreDirectory(false, coreName), THUMBNAILS_DIR);
        Path segmentPath = new Path(thumbnailPath, segment);
        return new Path(segmentPath, imgName);
    }

    public HashMap<String, MapFile.Reader[]> getSegmentToMapFileReaderMap(String coreName, String methodName) {

        HashMap<String, MapFile.Reader[]> mapfileReaders = new HashMap<String, MapFile.Reader[]>();
        List<String> segments = listSegments(coreName);

        try {
            Method m = this.getClass().getDeclaredMethod(methodName, new Class[] { Boolean.class, String.class, String.class });
            m.setAccessible(true);

            for(String segment : segments) {
                try {
                    Path dir = (Path) m.invoke(this, true, coreName, segment);
                    if (hdfsFileSystem.exists(dir)) {
                        mapfileReaders.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, dir, nutchConf));
                    }
                } catch (InvocationTargetException e) {
                    logger.error(e.getMessage());
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage());
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (NoSuchMethodException e) {
            logger.error(e.getMessage());
        }
        return mapfileReaders;
    }

    public void notifyOfThreadComplete(final Thread thread) {
        int index = thumbnailThreads.indexOf(thread);
        if (index >= 0) {
            thumbnailThreads.remove(index);
        }
        if (thumbnailThreads.size() == 0) {
            System.out.println("Done creating thumbnails.");
        }
    }

    public void generateThumbnails(String coreName) throws IOException {
        List<String> segments = listSegments(coreName);

        HashMap<String, MapFile.Reader[]> mapfileReaders = getSegmentToMapFileReaderMap(coreName, "getHDFSContentDirectory");

        Path thumbnailDir = returnOrCreateDirectory(new Path(getHDFSCoreDirectory(false, coreName), THUMBNAILS_DIR), hdfsFileSystem);
        HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(segments, nutchConf, hdfsFileSystem,
                getHDFSCrawlFetchDataFile(true, coreName, "00000"));

        for(int i = 0; i < N_THUMBNAIL_THREADS; i++) {
            GenerateThumbnailThread worker = new GenerateThumbnailThread(iter, thumbnailDir, coreName, hdfsFileSystem, mapfileReaders);
            worker.addListener(this);
            worker.setName("ThreadSeg_" + i);
            worker.start();
            thumbnailThreads.add(worker);
        }
    }

    class GenerateThumbnailThread extends NotifyingThread {
        HDFSNutchCoreFileIterator fileIterator;
        Path thumbnailDir;
        String coreName;
        FileSystem fs;
        HashMap<String, MapFile.Reader[]> mapfileReaders;
        HashMap<String, String> files;
        int N_FILES = 100;

        GenerateThumbnailThread(HDFSNutchCoreFileIterator fileIterator, Path thumbnailDir, String coreName, FileSystem fs,
                                HashMap<String, MapFile.Reader[]> mapfileReaders) {
            this.fileIterator = fileIterator;
            this.thumbnailDir = thumbnailDir;
            this.coreName = coreName;
            this.fs = fs;
            this.mapfileReaders = mapfileReaders;
        }

        private void createThumbnails() {
            try {
                for(Map.Entry<String, String> entry : this.files.entrySet()) {
                    String fileName = entry.getKey();
                    String segment = entry.getValue();
                    returnOrCreateDirectory(new Path(thumbnailDir, segment), fs);

                    Content content = getFileContents(segment, fileName, mapfileReaders);
                    String thumbnail = documentConversionService.convertContentToThumbnail(content, fileName);

                    if (!thumbnail.equals("") && !Utils.hasFileErrorMessage(thumbnail)) {
                        Path hdfsThumbnailPath = getHDFSThumbnailPathFromHDFSDocPath(coreName, segment, fileName);
                        if (fs.exists(hdfsThumbnailPath)) {
                            fs.delete(hdfsThumbnailPath, false);
                        }
                        fs.moveFromLocalFile(new Path(thumbnail), hdfsThumbnailPath);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        public void doRun() {
            while (!fileIterator.done()) {
                this.files = fileIterator.getNextNFileNames(N_FILES);
                createThumbnails();
            }
        }
    }

    private Content fileDoesNotExistContent() {
        Content content = new Content();
        Metadata metadata = new Metadata();
        metadata.add(FILE_DOES_NOT_EXIST_METADATA_KEY, "true");
        content.setMetadata(metadata);
        return content;
    }

    public Content getFileContents(String coreName, String segment, String fileName) throws IOException {

        //Configuration conf = getNutchConfiguration();
        //FileSystem fs = getHDFSFileSystem();
        Path hdfsContentDirectory = getHDFSContentDirectory(true, coreName, segment);
        if (!hdfsFileSystem.exists(hdfsContentDirectory)) {
            return fileDoesNotExistContent();
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsContentDirectory, nutchConf));
        return getFileContents(segment, fileName, map);
    }

    public Content getFileContents(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException {
        if (!map.containsKey(segment)) {
            return null;
        }

        Text key = new Text(fileName);
        Content content = new Content();
        MapFileOutputFormat.getEntry(map.get(segment), PARTITIONER, key, content);

        return content;
    }

    public ParseData getParseData(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException {
        if (!map.containsKey(segment)) {
            return null;
        }

        Text key = new Text(fileName);
        ParseData parseData = new ParseData();
        MapFileOutputFormat.getEntry(map.get(segment), PARTITIONER, key, parseData);

        return parseData;
    }

    public ParseData getParseData(String coreName, String segment, String fileName) throws IOException {
        Path hdfsParseDataDir = getHDFSParseDataDir(true, coreName, segment);
        if (!hdfsFileSystem.exists(hdfsParseDataDir)) {
            return null;
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsParseDataDir, nutchConf));
        return getParseData(segment, fileName, map);
    }

    public String getParsedText(String segment, String fileName, HashMap<String, MapFile.Reader[]> map) throws IOException {
        if (!map.containsKey(segment)) {
            return "";
        }

        Text key = new Text(fileName);
        ParseText parseText = new ParseText();
        MapFileOutputFormat.getEntry(map.get(segment), PARTITIONER, key, parseText);

        return Utils.getUTF8String(parseText.getText());
    }

    public String getParsedText(String coreName, String segment, String fileName) throws IOException {
        Path hdfsParseTextDir = getHDFSParseTextDir(true, coreName, segment);
        if (!hdfsFileSystem.exists(hdfsParseTextDir)) {
            return "";
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsParseTextDir, nutchConf));

        return getParsedText(segment, fileName, map);
    }

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException {
        List<Content> contents = new ArrayList<Content>();

        Path hdfsContentDirectory = getHDFSContentDirectory(true, coreName, segment);
        if (hdfsFileSystem.exists(hdfsContentDirectory)) {
            MapFile.Reader[] readers = MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsContentDirectory, nutchConf);
            for(String fileName : fileNames) {
                Text key = new Text(fileName);
                Content content = new Content();
                MapFileOutputFormat.getEntry(readers, PARTITIONER, key, content);
                contents.add(content);
            }
        }
        return contents;
    }

    public void testCrawlData(String coreName, String segment) {
        try {
            FileSystem fs = getHDFSFileSystem();
            Configuration conf = NutchConfiguration.create();

            List<String> segments = listSegments(coreName);

            HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(segments, conf, fs, getHDFSCrawlFetchDataFile(true, coreName, "00000"));
            iter.getNextNFileNames(100);

            for(String s : listSegments(coreName)) {
                TreeMap<Text, CrawlDatum> allContents = getCrawlData(getHDFSCrawlParseDataFile(true, coreName, s));
            }
            //crawlDbReader.processStatJob(getHDFSCrawlDBCurrentIndexFile(true, coreName).toString(), conf, true);
            //TreeMap<Text, CrawlDatum> allContents = getCrawlData(getHDFSCrawlDBCurrentDataFile(true, coreName));
           // getContents(getHDFSContentDataFile(true, coreName, segment), Content.class, HashMap<Text, Content>.class);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public Content getContent(String coreName, String segment, String filePath, StringWriter writer) throws IOException {
        Content content = getFileContents(coreName, segment, filePath);
        if (content == null) {
            Utils.printFileErrorMessage(writer, "Null content");
            return null;
        }
        if (content.getMetadata().get(FILE_DOES_NOT_EXIST_METADATA_KEY) != null) {
            Utils.printFileErrorMessage(writer, "File " + filePath + " does not exist on HDFS");
            return null;
        }
        return content;
    }


    public void printFileContents(String coreName, String segment, String fileName, StringWriter writer, boolean preview) {
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);

            g.writeStartObject();

            if (fileName.endsWith(".json")) {
                printJSONFileContents(coreName, segment, fileName, g, preview);
            } else {
                printImageFileContents(coreName, segment, fileName, g);
            }

            g.writeEndObject();
            g.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void printJSONFileContents(String coreName, String segment, String fileName, JsonGenerator g, boolean preview) {
        try {
            Content content = getContent(coreName, segment, fileName, new StringWriter());
            if (content == null) {
                return;
            }

            if (!content.getContentType().equals(Constants.JSON_CONTENT_TYPE)) {
                return;
            }

            g.writeStringField("url", content.getUrl());
            g.writeStringField("contentType", content.getContentType());
            g.writeStringField("dateAdded", content.getMetadata().get("Last-Modified"));

            List<String> previewFields = preview ? getHDFSPreviewFields(coreName) : null;
            JSONObject jsonObject = JSONObject.fromObject(new String(content.getContent()));
            JsonParsingUtils.printJSONObject(jsonObject, "Contents", "", previewFields, g);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public void printImageFileContents(String coreName, String segment, String fileName, JsonGenerator g) throws IOException {
        Path hdfsImgPath = getHDFSThumbnailPathFromHDFSDocPath(coreName, segment, fileName);
        byte[] content = getFileContentsAsBytes(hdfsImgPath);
        String base64String = Base64.encodeBase64String(content);

        g.writeStringField("url", fileName);
        g.writeStringField("contentType", "image");
        g.writeStringField("Contents", "data:image/png;base64," + base64String);
    }

    public String getContentTypeFromParseData(ParseData parseData){
        String contentType = parseData.getContentMeta().get(HttpHeaders.CONTENT_TYPE);
        if (Utils.stringIsNullOrEmpty(contentType)) {
            contentType = parseData.getParseMeta().get(HttpHeaders.CONTENT_TYPE);
        }
        return (contentType == null) ? "" : contentType;
    }
}
