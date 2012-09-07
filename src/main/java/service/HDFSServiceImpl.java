package service;

import LucidWorksApp.utils.JsonParsingUtils;
import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapFileOutputFormat;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.stereotype.Service;


import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

@Service
public class HDFSServiceImpl implements HDFSService {

    private static final String HDFS_ERROR_STRING = "ERROR";
    private static final String FACETFIELDS_HDFSFILENAME = "fields.csv";
    private static final String HDFS_URI = "hdfs://denlx006.dn.gates.com:8020";
    private static final String CRAWL_DIR = "/user/hdfs/crawl";
    private static final String SEGMENTS_DIR = CRAWL_DIR + "/segments";
    private static final String CONTENT_DATA = Content.DIR_NAME + "/part-00000/data";
    private static final String CRAWL_FETCH_DATA = CrawlDatum.FETCH_DIR_NAME + "/part-00000/data";


    private static final Partitioner PARTITIONER = new HashPartitioner();

    private String hdfsSegmentDirectory(String hdfsDate) {
        return HDFS_URI + SEGMENTS_DIR + "/" + hdfsDate;
    }

    private Configuration getHDFSConfiguration() {
        return new Configuration();
        /*
        config.addResource(new Path(hadoopDirectory + "/conf/hadoop-env.sh"));
        config.addResource(new Path(hadoopDirectory + "/conf/core-site.xml"));
        config.addResource(new Path(hadoopDirectory + "/conf/hdfs-site.xml"));
        config.addResource(new Path(hadoopDirectory + "/conf/mapred-site.xml"));
        */
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

    public String getFileContents(String remoteFilePath) {
        StringBuilder sb = new StringBuilder();

        try {
            FileSystem fs = getHDFSFileSystem();
            Path path = new Path(remoteFilePath);

            if (!fs.exists(path)) {
                return HDFS_ERROR_STRING + "File does not exist";
            }

            String line = "";
            DataInputStream d = new DataInputStream(fs.open(path));
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

    public JSONObject getJSONFileContents(String remoteFilePath) {
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

    public TreeMap<String, String> getHDFSFacetFields(String hdfsDir) {
        TreeMap<String, String> namesAndTypes = new TreeMap<String, String>();

        hdfsDir = hdfsDir.endsWith("/") ? hdfsDir : hdfsDir + "/";
        String response = this.getFileContents(hdfsDir + FACETFIELDS_HDFSFILENAME);

        if (!response.startsWith(HDFS_ERROR_STRING)) {
            for(String ret : response.split(",")) {
                String[] n = ret.split(":");
                namesAndTypes.put(n[0], n[1]);
            }
        }

        return namesAndTypes;
    }

    public List<String> listFiles(String hdfsDirectory) {
        List<String> filePaths = new ArrayList<String>();

        FileSystem fs = getHDFSFileSystem();
        Path path = new Path(hdfsDirectory);
        int hdfsUriStringLength = HDFS_URI.length();

        try {
            RemoteIterator<LocatedFileStatus> r = fs.listFiles(path, true);
            while(r.hasNext()) {
                String pathString = r.next().getPath().toString();
                // just add the HDFS path: hdfs://denlx006.dn.gates.com/path/etc --> /path/etc
                filePaths.add(pathString.substring(hdfsUriStringLength));
            }
        } catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return filePaths;
    }

    private HashMap<Text, CrawlDatum> getCrawlData(String hdfsDate, String crawlDir) throws IOException {
        HashMap<Text, CrawlDatum> allContents = new HashMap<Text, CrawlDatum>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = getHDFSFileSystem();

        Path path = new Path(hdfsSegmentDirectory(hdfsDate), crawlDir);
        SequenceFile.Reader reader= new SequenceFile.Reader(fs, path, conf);

        do {
            Text key = new Text();
            CrawlDatum value = new CrawlDatum();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while(true);

        return allContents;
    }

    private HashMap<Text, Content> getAllContents(String hdfsDate) throws IOException {
        HashMap<Text, Content> allContents = new HashMap<Text, Content>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = getHDFSFileSystem();

        Path contentData = new Path(hdfsSegmentDirectory(hdfsDate), CONTENT_DATA);
        SequenceFile.Reader reader= new SequenceFile.Reader(fs, contentData, conf);

        do {
            Text key = new Text();
            Content value = new Content();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while(true);

        return allContents;
    }

    private Content getFileContents(String hdfsDate, String fileName) throws IOException {

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = getHDFSFileSystem();

        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, new Path(hdfsSegmentDirectory(hdfsDate), Content.DIR_NAME), conf);
        Text key = new Text(fileName);
        Content content = new Content();
        MapFileOutputFormat.getEntry(readers, PARTITIONER, key, content);

        return content;
    }

    public void printFileContents(String hdfsDate, String fileName, JsonGenerator g) {
        try {
            HashMap<Text, Content> allContents = getAllContents(hdfsDate);
            HashMap<Text, CrawlDatum> allCrawlFetchData = getCrawlData(hdfsDate, CRAWL_FETCH_DATA);
            HashMap<Text, CrawlDatum> allCrawlGenData = getCrawlData(hdfsDate, CrawlDatum.GENERATE_DIR_NAME + "/part-00000");
            Content content = getFileContents(hdfsDate, "file:/opt/omega/smalltest/out100010.txt");
            if (content == null) {
                return;
            }

            g.writeStartObject();

            Metadata metadata = content.getMetadata();
            for(String name : metadata.names()) {
                g.writeArrayFieldStart(name);
                for(String value : metadata.getValues(name)) {
                    g.writeString(value);
                }
                g.writeEndArray();
            }

            String contentType = content.getContentType();
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
