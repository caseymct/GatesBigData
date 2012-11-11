package service;

import org.apache.nutch.parse.ParseData;

public interface SolrReindexService {

    public void reindexSolrCoreFromHDFS(String coreName);
}
