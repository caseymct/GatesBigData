package service;


import net.sf.json.JSONObject;

public interface HDFSService {

    public boolean addFile(String remoteFilePath, String localFilePath);

    public boolean removeFile(String remoteFilePath);

    public JSONObject getJSONFileContents(String fileName);

    public String getFileContents(String fileName);
}
