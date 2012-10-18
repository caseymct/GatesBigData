package service;

import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.util.*;

@Service
public class HDFSServiceImpl implements HDFSService {

    private DocumentConversionService documentConversionService;

    private static final String HDFS_ERROR_STRING = "ERROR";
    private static final String FACETFIELDS_HDFSFILENAME = "fields.csv";
    private static final String PREVIEWFIELDS_HDFSFILENAME = "previewfields.csv";
    private static final String HDFS_URI = "hdfs://denlx006.dn.gates.com:8020";
    //private static final String HDFS_URI = "hdfs://127.0.0.1:8020";

    private static final String USER_HDFS_DIR = "user/hdfs";
    private static final String CRAWL_DIR = "crawl";
    private static final String SEGMENTS_DIR = "segments";
    private static final String CRAWLDB_CURRENT = "crawldb/current";
    private static final String PART_00000 = "part-00000";
    private static final String PART_DATA = PART_00000 + "/data";
    private static final String THUMBNAILS_DIR = "thumbnails";

    private static final String CONTENT_DATA = Content.DIR_NAME + "/" + PART_DATA;
    private static final String CRAWL_FETCH_DATA = CrawlDatum.FETCH_DIR_NAME + "/" + PART_DATA;
    private static final String CRAWL_GENERATE_DATA = CrawlDatum.GENERATE_DIR_NAME + "/" + PART_00000;

    private static final String FILE_DOES_NOT_EXIST_METADATA_KEY = "FILE_DOES_NOT_EXIST";

    private static final Partitioner PARTITIONER = new HashPartitioner();

    @Autowired
    public void setServices(DocumentConversionService documentConversionService) {
        this.documentConversionService = documentConversionService;
    }

    public Path getHDFSCoreDirectory(boolean includeURI, String coreName) {
        return new Path((includeURI ? HDFS_URI : "") + "/" + USER_HDFS_DIR, coreName);
    }

    public Path getHDFSCrawlDirectory(boolean includeURI, String coreName) {
        return new Path(getHDFSCoreDirectory(includeURI, coreName), CRAWL_DIR);
    }

    public Path getHDFSSegmentsDirectory(boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), SEGMENTS_DIR);
    }

    public Path getHDFSSegmentDirectory(boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentsDirectory(includeURI, coreName), segment);
    }

    public Path getHDFSCrawlFetchDataFile(boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_FETCH_DATA);
    }

    public Path getHDFSCrawlGenerateFile(boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CRAWL_GENERATE_DATA);
    }

    public Path getHDFSContentDirectory(boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), Content.DIR_NAME);
    }

    public Path getHDFSContentDataFile(boolean includeURI, String coreName, String segment) {
        return new Path(getHDFSSegmentDirectory(includeURI, coreName, segment), CONTENT_DATA);
    }

    public Path getHDFSCrawlDBCurrentDataFile(boolean includeURI, String coreName) {
        return new Path(getHDFSCrawlDirectory(includeURI, coreName), CRAWLDB_CURRENT + "/" + PART_DATA);
    }

    public Path getHDFSFacetFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), FACETFIELDS_HDFSFILENAME);
    }

    public Path getHDFSPreviewFieldsCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(true, coreName), PREVIEWFIELDS_HDFSFILENAME);
    }

    private Configuration getHDFSConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setQuietMode(true);
        return configuration;
    }

    private Configuration getNutchConfiguration() {
        Configuration configuration = NutchConfiguration.create();
        configuration.setQuietMode(true);
        return configuration;
    }

    private FileSystem getHDFSFileSystem() {
        try {
            Configuration conf = getHDFSConfiguration();
            return FileSystem.get(URI.create(HDFS_URI), conf, "hdfs");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory) {
        int nAdded = 0;

        try {
            FileSystem fs = getHDFSFileSystem();
            File localFileDir = new File(localFileDirectory);
            File[] localFiles = localFileDir.listFiles();

            if (localFiles != null && localFiles.length > 0) {
                for(File file : localFiles) {

                    Path srcPath = new Path(file.getAbsolutePath());
                    Path dstPath = new Path(remoteFileDirectory + "/" + file.getName());

                    if (!fs.exists(dstPath)) {
                        try {
                            fs.copyFromLocalFile(srcPath, dstPath);
                            nAdded++;
                        } catch(Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                }
            }

            fs.close();
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

    public byte[] getFileContents(Path remoteFilePath) {
        byte[] contents = null;

        try {
            FileSystem fs = getHDFSFileSystem();
            if (!fs.exists(remoteFilePath)) {
                return new byte[0];
            }

            long length = fs.getContentSummary(remoteFilePath).getLength();
            if (length > Integer.MAX_VALUE) {
                System.out.println("File is too large. ");
                return new byte[0];
            }

            DataInputStream d = new DataInputStream(fs.open(remoteFilePath));

            contents = new byte[(int)length];

            // Read in the bytes
            int offset = 0, numRead = 0;
            while (offset < contents.length
                    && (numRead= d.read(contents, offset, contents.length-offset)) >= 0) {
                offset += numRead;
            }

            // Ensure all the bytes have been read in
            if (offset < contents.length) {
                throw new IOException("Could not completely read file " + remoteFilePath);
            }

            d.close();
            /*
            DataInputStream d = new DataInputStream(fs.open(remoteFilePath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(d));

            while ((reader.readLine()) != null){
            }
            reader.close();
            d.close();
            fs.close();
             */
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return contents;
    }

    public TreeMap<String, String> getHDFSFacetFields(String coreName) {
        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();

        String response = new String(getFileContents(getHDFSFacetFieldsCustomFile(coreName)));
        if (!response.startsWith(HDFS_ERROR_STRING)) {
            for(String ret : response.split(",")) {
                String[] n = ret.split(":");
                namesAndTypes.put(n[0], n[1]);
            }
        }

        return namesAndTypes;
    }

    public List<String> getHDFSPreviewFields(String coreName) {
        List<String> fields = new ArrayList<String>();
        String response = new String(getFileContents(getHDFSPreviewFieldsCustomFile(coreName)));
        if (!response.startsWith(HDFS_ERROR_STRING)) {
            fields = Arrays.asList(response.split(","));
        }

        return fields;
    }

    public List<String> listFiles(Path hdfsDirectory, boolean recurse) {
        List<String> filePaths = new ArrayList<String>();

        FileSystem fs = getHDFSFileSystem();
        int hdfsUriStringLength = HDFS_URI.length();

        try {
            if (recurse) {
                RemoteIterator<LocatedFileStatus> r = fs.listFiles(hdfsDirectory, true);
                while(r.hasNext()) {
                    String pathString = r.next().getPath().toString();
                    // just add the HDFS path: hdfs://denlx006.dn.gates.com/path/etc --> /path/etc
                    filePaths.add(pathString.substring(hdfsUriStringLength));
                }
            } else {
                for(FileStatus status : fs.listStatus(hdfsDirectory)) {
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

    private long getNumberOfFilesCrawled(String coreName) {
        long total = 0;

        try {
            for(String segment : listSegments(coreName)) {
                total += getCrawlData(getHDFSCrawlGenerateFile(true, coreName, segment)).size();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return total;
    }

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer) {
        long total = 0;

        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);

            g.writeStartObject();

            for(String segment : listSegments(coreName)) {
                g.writeArrayFieldStart(segment);

                for(Map.Entry entry : getCrawlData(getHDFSCrawlGenerateFile(true, coreName, segment)).entrySet()) {
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

        SequenceFile.Reader reader= new SequenceFile.Reader(getHDFSFileSystem(), path, getNutchConfiguration());

        do {
            Text key = new Text();
            CrawlDatum value = new CrawlDatum();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while (true);

        return allContents;
    }

    public HashMap<Text, Content> getAllContents(String coreName) throws IOException {
        HashMap<Text, Content> allContents = new HashMap<Text, Content>();

        Configuration conf = getNutchConfiguration();
        FileSystem fs = getHDFSFileSystem();

        for(String segment : listSegments(coreName)) {
            Path contentData = getHDFSContentDataFile(true, coreName, segment);
            SequenceFile.Reader reader= new SequenceFile.Reader(fs, contentData, conf);

            do {
                Text key = new Text();
                Content value = new Content();
                if (!reader.next(key, value)) break;
                allContents.put(key, value);
            } while(true);
        }

        return allContents;
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

    public void generateThumbnails(String coreName) throws IOException {

        Configuration conf = getNutchConfiguration();
        FileSystem fs = getHDFSFileSystem();

        Path thumbnailDir = returnOrCreateDirectory(new Path(getHDFSCoreDirectory(false, coreName), THUMBNAILS_DIR), fs);

        for(String segment : listSegments(coreName)) {
            returnOrCreateDirectory(new Path(thumbnailDir, segment), fs);

            Path contentData = getHDFSContentDataFile(true, coreName, segment);
            SequenceFile.Reader reader= new SequenceFile.Reader(fs, contentData, conf);

            do {
                Text key = new Text();
                Content value = new Content();
                if (!reader.next(key, value)) break;

                String thumbnail = documentConversionService.convertContentToThumbnail(value, key);
                if (!thumbnail.equals("") && !Utils.hasFileErrorMessage(thumbnail)) {
                    fs.moveFromLocalFile(new Path(thumbnail),
                            getHDFSThumbnailPathFromHDFSDocPath(coreName, segment, key.toString()));
                }
            } while(true);
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

        Configuration conf = getNutchConfiguration();
        FileSystem fs = getHDFSFileSystem();
        Path hdfsContentDirectory = getHDFSContentDirectory(true, coreName, segment);
        if (!fs.exists(hdfsContentDirectory)) {
            return fileDoesNotExistContent();
        }

        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, hdfsContentDirectory, conf);
        Text key = new Text(fileName);
        Content content = new Content();
        MapFileOutputFormat.getEntry(readers, PARTITIONER, key, content);

        return content;
    }

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException {
        List<Content> contents = new ArrayList<Content>();

        Configuration conf = getNutchConfiguration();
        FileSystem fs = getHDFSFileSystem();
        Path hdfsContentDirectory = getHDFSContentDirectory(true, coreName, segment);

        if (fs.exists(hdfsContentDirectory)) {
            MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, hdfsContentDirectory, conf);
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
            HashMap<Text, Content> allContents = getAllContents(coreName);

            for(Map.Entry<Text,Content> entry : allContents.entrySet()) {
                Content content = entry.getValue();
                Text key = entry.getKey();
            }
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
            Content content = getContent(coreName, segment, fileName, writer);
            if (content == null) {
                return;
            }

            List<String> previewFields = preview ? getHDFSPreviewFields(coreName) : null;

            if (content.getContentType().equals("application/json")) {
                printJSONFileContents(writer, content, previewFields);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void printJSONFileContents(StringWriter writer, Content content, List<String> previewFields) throws IOException {
        JsonFactory f = new JsonFactory();
        JsonGenerator g = f.createJsonGenerator(writer);

        g.writeStartObject();
        g.writeStringField("url", content.getUrl());
        g.writeStringField("contentType", content.getContentType());
        g.writeStringField("dateAdded", content.getMetadata().get("Last-Modified"));

        JSONObject jsonObject = JSONObject.fromObject(new String(content.getContent()));
        JsonParsingUtils.printJSONObject(jsonObject, "Contents", "", previewFields, g);

        g.writeEndObject();
        g.close();
    }
}
