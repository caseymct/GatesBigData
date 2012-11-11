package service;

import LucidWorksApp.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SolrReindexServiceImpl implements SolrReindexService {

    private CoreService coreService;
    private HDFSService hdfsService;

    private static final Logger logger = Logger.getLogger(SolrReindexServiceImpl.class);

    @Autowired
    public void setServices(CoreService coreService, HDFSService hdfsService) {
        this.coreService = coreService;
        this.hdfsService = hdfsService;
    }

    public void reindexSolrCoreFromHDFS(String coreName) {
        List<Thread> threads = new ArrayList<Thread>();

        for(String segment : hdfsService.listSegments(coreName)) {
            Thread worker = new SolrIndexThread(coreName, segment);
            worker.setName("ThreadSeg_" + segment);
            worker.start();
            threads.add(worker);
        }
    }

    class SolrIndexThread extends Thread {
        String coreName;
        String segment;

        List<SolrInputDocument> docList = new ArrayList<SolrInputDocument>();

        SolrIndexThread(String coreName, String segment) {
            this.coreName = coreName;
            this.segment = segment;
        }

        public void run() {
            Configuration conf = hdfsService.getNutchConfiguration();
            FileSystem fs = hdfsService.getHDFSFileSystem();

            try {
                Path parseDataFile   = hdfsService.getHDFSParseDataFile(true, coreName, segment);
                SequenceFile.Reader parseDataReader = new SequenceFile.Reader(fs, parseDataFile, conf);
                do {
                    Text parseKey = new Text();
                    ParseData parseData = new ParseData();
                    if (!parseDataReader.next(parseKey, parseData)) break;
                    if (!parseData.getStatus().isSuccess()) continue;

                    String url = Utils.decodeUrl(parseKey.toString());
                    if (!url.endsWith("/")) {
                        String contentType = hdfsService.getContentTypeFromParseData(parseData);
                        Content content = null;
                        if (contentType.equals("application/json")) {
                            content = hdfsService.getFileContents(coreName, segment, parseKey.toString());
                        }
                        SolrInputDocument doc = coreService.createSolrInputDocumentFromNutch(url, parseData, segment,
                                coreName, contentType, content);
                        if (doc != null) {
                            docList.add(doc);
                        }
                    }
                } while(true);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            System.out.println("Adding documents for segment " + segment);
            coreService.addDocumentToSolrIndex(docList, coreName);
        }
    }
}
