package service;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

@Service
public class HDFSServiceImpl implements HDFSService {

    private static final String hadoopDirectory = "/Users/caseymctaggart/projects/hadoop/hadoop-0.20.0";
    private static final String ERROR = "!Error!";

    private Configuration getHDFSConfiguration() {
        Configuration config = new Configuration();
        config.addResource(new Path(hadoopDirectory + "/conf/hadoop-env.sh"));
        config.addResource(new Path(hadoopDirectory + "/conf/core-site.xml"));
        config.addResource(new Path(hadoopDirectory + "/conf/hdfs-site.xml"));
        config.addResource(new Path(hadoopDirectory + "/conf/mapred-site.xml"));

        return config;
    }

    public boolean addFile(String remoteFilePath, String localFilePath) {
        try {
            Configuration conf = getHDFSConfiguration();

            FileSystem fs = FileSystem.get(conf);
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
            Configuration conf = getHDFSConfiguration();
            FileSystem fs = FileSystem.get(conf);
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
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);
            Path path = new Path(remoteFilePath);

            if (!fs.exists(path)) {
                return ERROR + "File does not exist";
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

        if (fileContents.contains(ERROR)) {
            error.put("Error", fileContents.substring(ERROR.length()));
            return error;
        }

        try {
            return JSONObject.fromObject(fileContents);
        } catch (JSONException e) {
            error.put("Error", "Cannot convert contents to JSON");
            return error;
        }
    }
}
