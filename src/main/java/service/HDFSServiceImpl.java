package service;

import LucidWorksApp.utils.JsonParsingUtils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.util.*;

@Service
public class HDFSServiceImpl implements HDFSService {

    private static final String HDFS_ERROR_STRING = "ERROR";
    private static final String FACETFIELDS_HDFSFILENAME = "fields.csv";
    private static final String HDFS_URI = "hdfs://denlx006.dn.gates.com:8020";
    //private static final String HDFS_URI = "hdfs://127.0.0.1:8020";

    private static final String USER_HDFS_DIR = "/user/hdfs";
    private static final String CRAWL_DIR = USER_HDFS_DIR + "/crawl";
    private static final String SEGMENTS_DIR = CRAWL_DIR + "/segments";
    private static final String CRAWLDB_CURRENT = CRAWL_DIR + "/crawldb/current";

    private static final String CONTENT_DATA = Content.DIR_NAME + "/part-00000/data";
    private static final String CRAWL_FETCH_DATA = CrawlDatum.FETCH_DIR_NAME + "/part-00000/data";
    private static final String CRAWL_GENERATE_DATA = CrawlDatum.GENERATE_DIR_NAME + "/part-00000";

    private static final Partitioner PARTITIONER = new HashPartitioner();

    public Path getHDFSSegmentsDirectory(boolean includeURI) {
        return new Path((includeURI ? HDFS_URI : "") + SEGMENTS_DIR);
    }

    public Path getHDFSSegmentDirectory(String segment, boolean includeURI) {
        return new Path(getHDFSSegmentsDirectory(includeURI), segment);
    }

    public Path getHDFSCrawlFetchDataFile(String segment, boolean includeURI) {
        return new Path(getHDFSSegmentDirectory(segment, includeURI), CRAWL_FETCH_DATA);
    }

    public Path getHDFSCrawlGenerateFile(String segment, boolean includeURI) {
        return new Path(getHDFSSegmentDirectory(segment, includeURI), CRAWL_GENERATE_DATA);
    }

    public Path getHDFSContentDirectory(String segment, boolean includeURI) {
        return new Path(getHDFSSegmentDirectory(segment, includeURI), Content.DIR_NAME);
    }

    public Path getHDFSContentDataFile(String segment, boolean includeURI) {
        return new Path(getHDFSSegmentDirectory(segment, includeURI), CONTENT_DATA);
    }

    public Path getHDFSCoreDirectory(String coreName) {
        return new Path(USER_HDFS_DIR, coreName);
    }

    public Path getHDFSFacetFieldCustomFile(String coreName) {
        return new Path(getHDFSCoreDirectory(coreName), FACETFIELDS_HDFSFILENAME);
    }

    public Path getHDFSCrawlDirectory(boolean includeURI) {
        return new Path((includeURI ? HDFS_URI : "") + CRAWL_DIR);
    }

    private Configuration getHDFSConfiguration() {
        return new Configuration();
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

    public String getFileContents(Path remoteFilePath) {
        StringBuilder sb = new StringBuilder();

        try {
            FileSystem fs = getHDFSFileSystem();

            if (!fs.exists(remoteFilePath)) {
                return HDFS_ERROR_STRING + "File does not exist";
            }

            String line = "";
            DataInputStream d = new DataInputStream(fs.open(remoteFilePath));
            BufferedReader reader = new BufferedReader(new InputStreamReader(d));

            while ((line = reader.readLine()) != null){
                sb.append(line);
            }
            reader.close();
            d.close();
            fs.close();

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return sb.toString();
    }

    public JSONObject getJSONFileContents(Path remoteFilePath) {
        JSONObject error = new JSONObject();

        String fileContents = getFileContents(remoteFilePath);

        if (fileContents.contains(HDFS_ERROR_STRING)) {
            error.put("Error", fileContents.substring(HDFS_ERROR_STRING.length()));
            return error;
        }

        try {
            return JSONObject.fromObject(fileContents);
        } catch (JSONException e) {
            error.put("Error", "Cannot convert contents to JSON");
            return error;
        }
    }

    public TreeMap<String, String> getHDFSFacetFields(String coreName) {
        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();

        String response = getFileContents(getHDFSFacetFieldCustomFile(coreName));
        if (!response.startsWith(HDFS_ERROR_STRING)) {
            for(String ret : response.split(",")) {
                String[] n = ret.split(":");
                namesAndTypes.put(n[0], n[1]);
            }
        }

        return namesAndTypes;
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

    public List<String> listSegments() {
        return listFiles(getHDFSSegmentsDirectory(false), false);
    }

    public TreeMap<String, String> listFilesInCrawlDirectory() {
        int total = 0;
        TreeMap<String, String> filesCrawled = new TreeMap<String, String>();

        try {
            for(String segment : listSegments()) {
                List<String> files = new ArrayList<String>();
                for(Map.Entry entry : getCrawlData(getHDFSCrawlGenerateFile(segment, true)).entrySet()) {
                    files.add(entry.getKey().toString());
                }
                filesCrawled.put(segment, StringUtils.join(files, ","));
                total += files.size();
            }
        } catch (IOException e) {
            System.out.println(e);
        }
        filesCrawled.put("Total", total + "");

        return filesCrawled;
    }

    private HashMap<Text, CrawlDatum> getCrawlData(Path path) throws IOException {
        HashMap<Text, CrawlDatum> allContents = new HashMap<Text, CrawlDatum>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = getHDFSFileSystem();

        SequenceFile.Reader reader= new SequenceFile.Reader(fs, path, conf);

        do {
            Text key = new Text();
            CrawlDatum value = new CrawlDatum();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while(true);

        return allContents;
    }

    private HashMap<Text, Content> getAllContents(String segment) throws IOException {
        HashMap<Text, Content> allContents = new HashMap<Text, Content>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = getHDFSFileSystem();

        Path contentData = new Path(getHDFSSegmentDirectory(segment, true), CONTENT_DATA);
        SequenceFile.Reader reader= new SequenceFile.Reader(fs, contentData, conf);

        do {
            Text key = new Text();
            Content value = new Content();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while(true);

        return allContents;
    }

    private Content getFileContents(String segment, String fileName) throws IOException {

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = getHDFSFileSystem();

        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, getHDFSContentDirectory(segment, true), conf);
        Text key = new Text(fileName);
        Content content = new Content();
        MapFileOutputFormat.getEntry(readers, PARTITIONER, key, content);

        return content;
    }

    public void testCrawlData(String segment) {
        try {

            HashMap<Text, CrawlDatum> crawlDbCurr = getCrawlData(new Path(CRAWLDB_CURRENT + "/part-00000/data"));
        //    HashMap<Text, CrawlDatum> allCrawlFetchData = getCrawlData(segment, CRAWL_FETCH_DATA);
        //    HashMap<Text, CrawlDatum> allCrawlGenData = getCrawlData(segment, CrawlDatum.GENERATE_DIR_NAME + "/part-00000");

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    public void printFileContents(String segment, String fileName, JsonGenerator g) {
        try {
            Content content = getFileContents(segment, fileName);
            if (content == null) {
                return;
            }

            String contentType = content.getContentType();

            g.writeStartObject();
            g.writeStringField("url", content.getUrl());
            g.writeStringField("contentType", content.getContentType());
            g.writeStringField("dateAdded", content.getMetadata().get("Last-Modified"));

            /*Metadata metadata = content.getMetadata();
            for(String name : metadata.names()) {
                g.writeArrayFieldStart(name);
                for(String value : metadata.getValues(name)) {
                    g.writeString(value);
                }
                g.writeEndArray();
            }*/

            if (contentType.equals("application/json")) {
                JSONObject jsonObject = JSONObject.fromObject(new String(content.getContent()));
                JsonParsingUtils.printJSONObject(jsonObject, "Contents", g);

            } else if (contentType.equals("text/plain")) {
                g.writeStringField("contents", new String(content.getContent()));
            }

            g.writeEndObject();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
