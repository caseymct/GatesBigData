package service;


import net.sf.json.JSONObject;

import java.util.List;
import java.util.TreeMap;

public interface HDFSService {

    public TreeMap<String, String> getHDFSFacetFields(String hdfsDir);

    public boolean addFile(String remoteFilePath, String localFilePath);

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory);

    public boolean removeFile(String remoteFilePath);

    public JSONObject getJSONFileContents(String fileName);

    public String getFileContents(String fileName);

    public List<String> listFiles(String hdfsDirectory);

    public void readOffNutch();
}
