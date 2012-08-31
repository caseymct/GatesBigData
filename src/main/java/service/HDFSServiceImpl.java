package service;

import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.segment.SegmentReader;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
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
            return FileSystem.get(URI.create(Utils.getHDFSUri()), conf, "hdfs");
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
        int hdfsUriStringLength = Utils.getHDFSUri().length();

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

    public void readOffNutch() {
        try {
            Configuration conf = NutchConfiguration.create();
            //CrawlDbReader dbr = new CrawlDbReader();

            //String crawlDb = "/user/hdfs/urls/crawldb/";

            String url="out100010.json";
            /*CrawlDatum res = dbr.get(crawlDb, url, conf);
            System.out.println("URL: " + url);
            if (res != null) {
                System.out.println(res);
            } else {
                System.out.println("not found");
            }  */

            FileSystem fs = getHDFSFileSystem();
            boolean co = true,     // Content
                    fe = false,    // Crawl Fetch
                    ge = false,    // Crawl Generate
                    pa = false,    // Crawl Parse
                    pd = true,     // ParseData
                    pt = false;    // ParseText

            Text key=new Text();
            Generator g = new Generator(conf);
            String segName = g.generateSegmentName();
            Path hdfsPath = new Path("/user/hdfs/urls/segments/" + segName + "/crawl_fetch/part-00000/index");
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/user/hdfs/urls/segments/20120831142157/crawl_fetch/part-00000/index"), conf);
            reader.next(key);

            reader=new SequenceFile.Reader(fs, new Path("/user/hdfs/urls/segments/" + segName + "/content/part-00000/data"), conf);
            reader.next(key);
            //Path localPath = new Path("C:\\Users\\cm0607\\projects\\LucidWorksApp\\segOut");


            //    Text key=new Text();
            //    CrawlDatum value=new CrawlDatum();
            //    if(!reader.next(key, value))
            Path testdir = new Path("/user/hdfs/urls");

            Path urlPath = new Path(testdir,"urls");
            Path crawldbPath = new Path(testdir,"crawldb");
            Path segmentsPath = new Path(testdir,"segments");
            LockUtil.removeLockFile(fs, new Path("/user/hdfs/urls/crawldb/.locked"));
            Path[] generatedSegment = g.generate(crawldbPath, segmentsPath, 1, Long.MAX_VALUE, Long.MAX_VALUE, false, true);
            Path content = new Path(new Path(generatedSegment[0], Content.DIR_NAME),"part-00000/data");
            //Path content = new Path("/user/hdfs/urls/segments/20120830142310/content/part-00000/data");
            reader=new SequenceFile.Reader(fs, content, conf);


            READ_CONTENT:
            do {
                 key=new Text();
                Content value=new Content();
                if(!reader.next(key, value)) break READ_CONTENT;
                String contentString=new String(value.getContent());
                if(contentString.indexOf("Nutch fetcher test page")!=-1) {
                    System.out.println(key.toString());
                }
            } while(true);

            reader.close();
            //SequenceFile.Reader reader = new SequenceFile.Reader(fs, hdfsPath, conf);
             key=new Text();
            CrawlDatum value=new CrawlDatum();
            while(reader.next(key, value)) {
                System.out.println(key.toString());
            }

            SegmentReader segmentReader = new SegmentReader(conf, co, fe, ge, pa, pd, pt);

            try {
            segmentReader.get(new Path("/user/hdfs/urls/segments/20120830142310"),
                new Text(url),
                new OutputStreamWriter(System.out, "UTF-8"),
                new HashMap<String, List<Writable>>());
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
