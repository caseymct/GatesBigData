package model;

import LucidWorksApp.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDFSNutchCoreFileIterator {
    private List<String> segments;
    private String currSegment;
    private int currSegmentIndex = 0;
    private Path currentCrawlFetchDataFile;
    private SequenceFile.Reader currentReader;
    private Configuration conf;
    private FileSystem fileSystem;
    private String CRAWL_FETCH_DIR = CrawlDatum.FETCH_DIR_NAME;
    private String SEGMENTS_DIR = "segments";
    private Pattern p = Pattern.compile(".*\\/" + SEGMENTS_DIR + "\\/([0-9]+)\\/" + CRAWL_FETCH_DIR + "\\/.*");

    public boolean done = false;

    public HDFSNutchCoreFileIterator(List<String> segments, Configuration conf, FileSystem fileSystem, Path currentCrawlFetchDataFile) {
        this.segments = segments;
        this.currSegment = segments.get(0);
        this.conf = conf;
        this.fileSystem = fileSystem;
        this.currentCrawlFetchDataFile = currentCrawlFetchDataFile;
        setCurrentReader();
    }

    private String crawlParseSubstring(String seg) {
        return StringUtils.join(new String[] {SEGMENTS_DIR, seg, CRAWL_FETCH_DIR}, "/");
    }

    private synchronized void updateCrawlFetchDataFile() {
        String s = this.currentCrawlFetchDataFile.toString();

        Matcher m = p.matcher(s);
        if (m.matches()) {
            s = s.replace(crawlParseSubstring(m.group(1)), crawlParseSubstring(this.currSegment));
        }
        this.currentCrawlFetchDataFile = new Path(s);
    }

    private synchronized void setCurrentReader() {
        updateCrawlFetchDataFile();
        try {
            currentReader = new SequenceFile.Reader(this.fileSystem, this.currentCrawlFetchDataFile, conf);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public boolean done() {
        return this.done;
    }

    public synchronized HashMap<String, String> getNextNFileNames(int n) {

        HashMap<String, String> map = new HashMap<String, String>();

        while( !this.done && map.size() < n) {
            Text key = new Text();
            CrawlDatum value = new CrawlDatum();

            try {
                if (!this.currentReader.next(key, value)) {
                    this.done = (++this.currSegmentIndex == segments.size());
                    if (! this.done) {
                        this.currSegment = segments.get(currSegmentIndex);
                        setCurrentReader();
                    }
                } else if (value.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS) {
                    if (!Utils.decodeUrl(key.toString()).endsWith("/")) {
                        map.put(key.toString(), this.currSegment);
                    }
                }
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
        return map;
    }
}
