package service;


public interface SolrReindexService {

    public void reindexSolrCoreFromHDFS(String coreName, Integer nThreads, Integer nFiles);
}
