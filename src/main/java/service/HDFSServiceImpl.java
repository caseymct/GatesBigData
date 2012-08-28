package service;

import LucidWorksApp.utils.Utils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
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


}
