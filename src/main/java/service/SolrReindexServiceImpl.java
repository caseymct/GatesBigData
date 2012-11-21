package service;

import LucidWorksApp.utils.Utils;
import model.HDFSNutchCoreFileIterator;
import model.NotifyingThread;
import model.ThreadCompleteListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.log4j.Logger;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolrReindexServiceImpl implements SolrReindexService, ThreadCompleteListener {

    private CoreService coreService;
    private HDFSService hdfsService;
    private static final int N_THREADS = 10;
    private static final int N_FILES   = 100;
    private static final String TIKA_ERROR_STRING = "Can't retrieve Tika parser for mime-type application/octet-stream";
    private static final String HDFS_PARSE_DATA_DIR_FN = "getHDFSParseDataDir";
    private static final String HDFS_PARSE_TEXT_DIR_FN = "getHDFSParseTextDir";
    private static final Partitioner PARTITIONER = new HashPartitioner();
    private List<Thread> threads = new ArrayList<Thread>();

    private static final Logger logger = Logger.getLogger(SolrReindexServiceImpl.class);

    @Autowired
    public void setServices(CoreService coreService, HDFSService hdfsService) {
        this.coreService = coreService;
        this.hdfsService = hdfsService;
    }

    public void reindexSolrCoreFromHDFS(String coreName, Integer nThreads, Integer nFiles) {
        nThreads = (nThreads == null || nThreads < 1) ? N_THREADS : nThreads;
        nFiles   = (nFiles == null || nFiles < 1) ? N_FILES : nFiles;

        HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(hdfsService.listSegments(coreName),
                                                        hdfsService.getNutchConfiguration(), hdfsService.getHDFSFileSystem(),
                                                        hdfsService.getHDFSCrawlFetchDataFile(true, coreName, "00000"));

        HashMap<String, MapFile.Reader[]> parseDataReaders = hdfsService.getSegmentToMapFileReaderMap(coreName, HDFS_PARSE_DATA_DIR_FN);
        HashMap<String, MapFile.Reader[]> parseTextReaders = hdfsService.getSegmentToMapFileReaderMap(coreName, HDFS_PARSE_TEXT_DIR_FN);

        for(int i = 0; i < nThreads; i++) {
            SolrIndexThread worker = new SolrIndexThread(iter, coreName, nFiles, parseDataReaders, parseTextReaders);
            worker.setName("ReindexThreadSeg_" + i);
            worker.addListener(this);
            worker.start();
            System.out.println("Creating thread " + i);
            threads.add(worker);
        }
    }

    public void notifyOfThreadComplete(final Thread thread) {
        int index = threads.indexOf(thread);
        if (index >= 0) {
            threads.remove(index);
        }
        if (threads.size() == 0) {
            System.out.println("Done reindexing.");
        }
    }

    class SolrIndexThread extends NotifyingThread {
        String coreName;
        HDFSNutchCoreFileIterator fileIterator;
        HashMap<String, MapFile.Reader[]> parseDataReaders;
        HashMap<String, MapFile.Reader[]> parseTextReaders;
        HashMap<String, String> files;
        int nFiles;

        List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();

        SolrIndexThread(HDFSNutchCoreFileIterator fileIterator, String coreName, int nFiles,
                        HashMap<String, MapFile.Reader[]> parseDataReaders, HashMap<String, MapFile.Reader[]> parseTextReaders) {
            this.fileIterator = fileIterator;
            this.coreName = coreName;
            this.parseDataReaders = parseDataReaders;
            this.parseTextReaders = parseTextReaders;
            this.nFiles = nFiles;
        }

        public void reindexFiles() {
            if (this.files.size() == 0) {
                return;
            }

            int i = 0, j = 0, n = this.files.size();
            for(Map.Entry<String, String> entry : this.files.entrySet()) {
                String fileName = entry.getKey();
                String segment = entry.getValue();

                try {
                    ParseData parseData = hdfsService.getParseData(segment, fileName, parseDataReaders);
                    ParseStatus status = parseData.getStatus();
                    if (status == null) continue;

                    String msg = status.getMessage();
                    boolean parseError = (msg != null && msg.equals(TIKA_ERROR_STRING));

                    if (status.isSuccess() || parseError) {

                        String url = Utils.decodeUrl(fileName);
                        if (!url.endsWith("/")) {
                            String contentType = hdfsService.getContentTypeFromParseData(parseData);
                            String parsedText = parseError ? null : hdfsService.getParsedText(segment, fileName, parseTextReaders);

                            SolrInputDocument doc = coreService.createSolrInputDocumentFromNutch(url, parseData, segment,
                                    coreName, contentType, parsedText);
                            if (doc != null) {
                                docList.add(doc);
                            }
                            System.out.println(this.getName() + " -- Processing file " + (i++) + " of " + j + ", n " + n);
                        }
                        j++;
                    }

                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
            System.out.println(this.getName() + " adding " + docList.size() + " docs to Solr.");
            coreService.addDocumentToSolrIndex(docList, coreName);
        }

        public void doRun() {
            while (!fileIterator.done()) {
                this.files = fileIterator.getNextNFileNames(nFiles);
                reindexFiles();
            }
            logger.debug("Done reindexing for " + this.getName());
        }
    }
}
