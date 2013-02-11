package service;

import GatesBigData.utils.HDFSUtils;
import GatesBigData.utils.Utils;
import model.HDFSNutchCoreFileIterator;
import model.NotifyingThread;
import service.solrReindexer.SolrIndexAction;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.log4j.Logger;
import org.apache.nutch.crawl.NutchWritable;
//import org.apache.nutch.indexer.solr.SolrUtils;
import org.apache.nutch.parse.*;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;

public class SolrReindexServiceImpl extends Configured implements SolrReindexService, Mapper<Text, Writable, Text, NutchWritable>,
        Reducer<Text, NutchWritable, Text, SolrIndexAction> {

    private HDFSService hdfsService;
    private CoreService coreService;

    private static final int N_THREADS = 8; //# of cores
    private static final int N_FILES   = 100;
    private static final String TIKA_ERROR_STRING = "Can't retrieve Tika parser for mime-type application/octet-stream";
    private static final String HDFS_PARSE_DATA_DIR_FN = "getHDFSParseDataDir";
    private static final String HDFS_PARSE_TEXT_DIR_FN = "getHDFSParseTextDir";
    private List<Thread> threads = new ArrayList<Thread>();

    private static final Logger logger = Logger.getLogger(SolrReindexServiceImpl.class);

    long startTimeMillis;
    int nDocs = 0;
    // 19770 just printing, using FileIterator
    // 62868 using SequenceFile reader
    // 809494 iterating over parseText (then 1840843, but paused in debugger)
    @Autowired
    public void setServices(CoreService coreService, HDFSService hdfsService) {
        this.hdfsService = hdfsService;
        this.coreService = coreService;
    }

    public void configure(JobConf job) {
        setConf(job);
    }

    public void reindexSolrCoreFromHDFSOLD(String coreName, Integer nThreads, Integer nFiles) {
        nThreads = (nThreads == null || nThreads < 1) ? N_THREADS : nThreads;
        nFiles   = (nFiles == null || nFiles < 1) ? N_FILES : nFiles;

        List<String> segments = hdfsService.listSegments(coreName);
        HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(segments,
                                                        hdfsService.getNutchConfiguration(), hdfsService.getHDFSFileSystem(),
                                                        HDFSUtils.getHDFSCrawlFetchDataFile(true, coreName, "00000"));

        HashMap<String, MapFile.Reader[]> parseDataReaders = hdfsService.getSegmentToMapFileReaderMap(coreName, HDFS_PARSE_DATA_DIR_FN);
        HashMap<String, MapFile.Reader[]> parseTextReaders = hdfsService.getSegmentToMapFileReaderMap(coreName, HDFS_PARSE_TEXT_DIR_FN);

        startTimeMillis = System.currentTimeMillis();
        for(int i = 0; i < nThreads; i++) {
        //for(String segment: segments) {
            SolrIndexThread worker = new SolrIndexThread(iter, coreName, nFiles, parseDataReaders, parseTextReaders);
            //SolrIndexThread worker = new SolrIndexThread(segment, coreName, nFiles, null, parseTextReaders);
            worker.setName("ReindexThreadSeg_" + i);
            //worker.addListener(this);
            worker.start();
            System.out.println("Starting thread " + worker.getName());
            threads.add(worker);
        }
    }

    public void reindexSolrCoreFromHDFS(String coreName, Integer nThreads, Integer nFiles) {
        try {
            setupMRJob(coreName);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void map(Text key, Writable value, OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {
        System.out.println("Mapping key " + key.toString());
        output.collect(key, new NutchWritable(value));
    }

    public void reduce(Text key, Iterator<NutchWritable> values, OutputCollector<Text, SolrIndexAction> output, Reporter reporter) throws IOException {
        System.out.println("Reducing key " + key.toString());
    }


    public void setupMRJob(String coreName) throws IOException {
        long start = System.currentTimeMillis();

        Path tmp = new Path("/tmp_" + System.currentTimeMillis() + "-" + new Random().nextInt());

        //JobConf job = new JobConf(hdfsService.getHDFSConfiguration(), SolrReindexServiceImpl.class);
        /*JobConf job = new JobConf(SolrReindexServiceImpl.class);

        for (String segment : hdfsService.listSegments(coreName)) {
            FileInputFormat.addInputPath(job, HDFSUtils.getHDFSCrawlFetchDir(true, coreName, segment));
            FileInputFormat.addInputPath(job, HDFSUtils.getHDFSCrawlParseDir(true, coreName, segment));
            FileInputFormat.addInputPath(job, HDFSUtils.getHDFSParseDataDir(true, coreName, segment));
            FileInputFormat.addInputPath(job, HDFSUtils.getHDFSParseTextDir(true, coreName, segment));
        }

        job.setInputFormat(SequenceFileInputFormat.class);
        //job.set(SolrConstants.SERVER_URL, SolrUtils.getSolrServerURI(coreName));
        job.setJarByClass(SolrReindexServiceImpl.class);
        job.setJobName("Reindex core " + coreName);
        //FileInputFormat.addInputPath(job, HDFSUtils.getHDFSCrawlDBCurrentDir(true, coreName));

        job.setMapperClass(SolrReindexServiceImpl.class);
        job.setReducerClass(SolrReindexServiceImpl.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputFormat(IndexerOutputFormat.class);
        job.setMapOutputValueClass(NutchWritable.class);
        job.setOutputValueClass(NutchWritable.class);

        //job.set(SolrConstants.PARAMS, solrParams);
        //NutchIndexWriterFactory.addClassToConf(job, SolrWriter.class);
        //job.setReduceSpeculativeExecution(false);

        Path tmp = new Path("C:/tmp_" + System.currentTimeMillis() + "-" + new Random().nextInt());
        FileOutputFormat.setOutputPath(job, tmp);

        try {
            JobClient.runJob(job);
            // do the commits once and for all the reducers in one go
            //SolrServer solr =  SolrUtils.getHttpSolrServer(job);
            //solr.commit();

            long end = System.currentTimeMillis();
            logger.info("SolrIndexer: elapsed: " + TimingUtil.elapsedTime(start, end));
        }
        catch (Exception e){
            logger.error(e.toString());
        } finally {
           // FileSystem.get(job).delete(tmp, true);
        }  */
    }

    public void close() throws IOException { }

    public void notifyOfThreadComplete(final Thread thread) {
        nDocs += ((SolrIndexThread) thread).getNDocsIndexed();
        int index = threads.indexOf(thread);
        if (index >= 0) {
            threads.remove(index);
        }
        if (threads.size() == 0) {
            System.out.println("Done reindexing " + nDocs + " docs");
            System.out.println("Total ms: " + (System.currentTimeMillis() - startTimeMillis));
            System.out.flush();
        }
    }

    class SolrIndexThread extends NotifyingThread {
        String coreName;
        HDFSNutchCoreFileIterator fileIterator;
        String segment;
        int nDocsIndexed = 0;
        HashMap<String, MapFile.Reader[]> parseDataReaders;
        HashMap<String, MapFile.Reader[]> parseTextReaders;
        SequenceFile.Reader parseDataReader;
        SequenceFile.Reader parseTextReader;
        HashMap<String, String> files;
        int nFiles;

        List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();

        SolrIndexThread(HDFSNutchCoreFileIterator fileIterator, String coreName, int nFiles,
        //SolrIndexThread(String segment, String coreName, int nFiles,
                        HashMap<String, MapFile.Reader[]> parseDataReaders, HashMap<String, MapFile.Reader[]> parseTextReaders) {
            this.fileIterator = fileIterator;
            this.coreName = coreName;
            this.parseDataReaders = parseDataReaders;
            this.parseTextReaders = parseTextReaders;
            //parseDataReader = hdfsService.getSequenceFileReader(HDFSUtils.getHDFSParseDataFile(true, coreName, segment));
            //parseTextReader = hdfsService.getSequenceFileReader(HDFSUtils.getHDFSParseTextDataFile(true, coreName, segment));
            this.nFiles = nFiles;
        }

        public int getNDocsIndexed() {
            return nDocsIndexed;
        }

        public void reindexFiles() {
            /*do {
                Text key = new Text();
                ParseText parseText = new ParseText();
                if (!parseTextReader.next(key, parseText)) break;
                nDocsIndexed++;
            } while (true);   */

            if (this.files.size() == 0) {
                return;
            }

            int i = 0, j = 0, n = this.files.size();
            for(Map.Entry<String, String> entry : this.files.entrySet()) {
                String fileName = entry.getKey();
                String segment = entry.getValue();

                try {
                    ParseData parseData = HDFSUtils.getParseData(segment, fileName, parseDataReaders);
                    ParseStatus status = parseData.getStatus();
                    if (status == null) continue;

                    String msg = status.getMessage();
                    boolean parseError = (msg != null && msg.equals(TIKA_ERROR_STRING));

                    if (status.isSuccess() || parseError) {

                        String url = Utils.decodeUrl(fileName);
                        if (!url.endsWith("/")) {
                            String contentType = HDFSUtils.getContentTypeFromParseData(parseData);
                            String parsedText = parseError ? null : HDFSUtils.getParsedText(segment, fileName, parseTextReaders);
                            /*
                            SolrInputDocument doc = coreService.createSolrInputDocumentFromNutch(url, parseData, segment,
                                    coreName, contentType, parsedText);
                            if (doc != null) {
                                docList.add(doc);
                            }
                            System.out.println(this.getName() + " -- Processing file " + (i++) + " of " + j + ", n " + n);   */
                        }
                    }
                    nDocsIndexed++;

                    if (nDocsIndexed % 1000 == 0) {
                        System.out.println(this.getName() + " has indexed " + nDocsIndexed);
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }

                /*
                if (docList.size() == 10000) {
                    System.out.println(this.getName() + " adding " + docList.size() + " docs to Solr.");
                    coreService.addDocumentToSolrIndex(docList, coreName);
                    docList = new ArrayList<SolrInputDocument>();
                }  */
            }
            //System.out.println(this.getName() + " adding " + docList.size() + " docs to Solr.");
            //coreService.addDocumentToSolrIndex(docList, coreName);
        }

        public void doRun() {
            /*try {
                reindexFiles();
                System.out.println("Done with " + this.getName() + ", " + nDocsIndexed);
                System.out.flush();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
            */
            while (!fileIterator.done()) {
                this.files = fileIterator.getNextNFileNames(nFiles);
                System.out.println(this.getName() + " reindexing starting again with " + this.files.size());
                reindexFiles();
            }
            System.out.println("Done reindexing for " + this.getName());
        }
    }
}
