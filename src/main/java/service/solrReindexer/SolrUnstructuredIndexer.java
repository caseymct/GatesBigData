import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import org.apache.nutch.crawl.*;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;

public class SolrUnstructuredIndexer {
    public static SolrServer SERVER;
    public static final String SOLR_URL_BASE    = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String HDFS_URI         = "hdfs://denlx006.dn.gates.com:8020";
    public static String SOLR_URL               = "";
    public static Path CORE_PATH                = new Path("/user/hdfs");
    public static Path CRAWL_PATH               = null;
    public static Path SEGMENTS_PATH            = null;
    public static Path CRAWLDB_PATH             = null;
    public static Path LINKDB_PATH              = null;

    private static final Logger logger = Logger.getLogger(SolrUnstructuredIndexer.class);

    public static class Map extends Configured implements Mapper<Text, Writable, Text, NutchWritable> {

        public void configure(JobConf job) {
            setConf(job);
        }

        public void close() throws IOException { }

        public void map(Text key, Writable value, OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {
            output.collect(key, new NutchWritable(value));
        }
    }

    public static class Reduce extends Configured implements Reducer<Text, NutchWritable, Text, SolrInputDocument> {
        public static SolrServer SERVER;
        public static String SOLR_URL;
        public static SimpleDateFormat solrDateFormat  = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
        public static final String SMB_PROTOCOL_STRING = "smb://";
        public static final String SMB_DOMAIN          = "NA";
        public static final String SMB_USERNAME        = "bigdatasvc";
        public static final String SMB_PASSWORD        = "Crawl2012";
        public static final String TIKA_ERROR_STRING   = "Can't retrieve Tika parser for mime-type application/octet-stream";
        //private IndexingFilters filters;

        private NtlmPasswordAuthentication auth;

        HashMap<String, String> mapMetadataToSolrFields = new HashMap<String, String>() {{
            put("Last-Save-Date", "last_modified");
            put("Creation-Date", "creation_date");
            put("Last-Author", "last_author");
            put("Application-Name", "application_name");
            put("Author", "author");
            put("Company", "company");
            put("Company", "company");
            put("title", "title");
        }};

        public void configure(JobConf job) {
            setConf(job);
            //this.filters = new IndexingFilters(getConf());
            SOLR_URL = job.get("SOLR_URL");
            SERVER = new HttpSolrServer(SOLR_URL);
        }

        public void close() throws IOException { }

        public String getUTF8String(String s) {
            try {
                return new String(s.getBytes(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                return new String(s.getBytes());
            }
        }

        public String decodeUrl(String url) {
            try {
                url = URLDecoder.decode(url, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                logger.error("Could not decode URL: " + e.getMessage());
            }
            return url;
        }

        public static String getContentTypeFromParseData(ParseData parseData){
            String contentType = parseData.getContentMeta().get(HttpHeaders.CONTENT_TYPE);
            if (contentType == null || contentType.equals("")) {
                contentType = parseData.getParseMeta().get(HttpHeaders.CONTENT_TYPE);
            }
            return (contentType == null) ? "" : contentType;
        }

        private SolrInputDocument addParseMeta(Metadata metadata, SolrInputDocument doc) {
            List<String> names = Arrays.asList(metadata.names());
            for (java.util.Map.Entry<String,String> entry : mapMetadataToSolrFields.entrySet()) {
                if (names.contains(entry.getKey())) {
                    doc.addField(entry.getValue(), metadata.get(entry.getKey()));
                }
            }
            return doc;
        }

        private NtlmPasswordAuthentication getAuth() {
            if (this.auth == null) {
                this.auth = new NtlmPasswordAuthentication(SMB_DOMAIN, SMB_USERNAME, SMB_PASSWORD);
            }
            return this.auth;
        }

        private SolrInputDocument addFieldIfNull(SolrInputDocument doc, String field, Object value) {
            if (doc.getField(field) == null) {
                doc.addField(field, value);
            }
            return doc;
        }

        private SolrInputDocument addSMBDatesToDoc(SolrInputDocument doc, String url) {
            try {
                SmbFile file = new SmbFile(url, getAuth());
                if (file.exists()) {
                    doc = addFieldIfNull(doc, "last_modified", solrDateFormat.format(new Date(file.getLastModified())));
                    doc = addFieldIfNull(doc, "creation_date", solrDateFormat.format(new Date(file.createTime())));
                }
            } catch (SmbException e) {
                logger.error(e.getMessage());
            } catch (MalformedURLException e){
                logger.error(e.getMessage());
            }
            return doc;
        }

        public void reduce(Text key, Iterator<NutchWritable> values, OutputCollector<Text, SolrInputDocument> output,
                           Reporter reporter) throws IOException {

            Inlinks inlinks = null;
            CrawlDatum dbDatum = null;
            CrawlDatum fetchDatum = null;
            ParseData parseData = null;
            ParseText parseText = null;

            logger.info("Reducing key " + key.toString());
            while (values.hasNext()) {
                final Writable value = values.next().get(); // unwrap
                if (value instanceof Inlinks) {
                    inlinks = (Inlinks)value;
                } else if (value instanceof CrawlDatum) {
                    final CrawlDatum datum = (CrawlDatum)value;
                    if (CrawlDatum.hasDbStatus(datum)) {
                        dbDatum = datum;
                    } else if (CrawlDatum.hasFetchStatus(datum)) {
                        System.out.println(key.toString() + ", " + datum.getStatus());
                        fetchDatum = datum;

                    } else if (CrawlDatum.STATUS_LINKED == datum.getStatus() ||
                            CrawlDatum.STATUS_SIGNATURE == datum.getStatus() ||
                            CrawlDatum.STATUS_PARSE_META == datum.getStatus()) {
                        continue;
                    } else {
                        throw new RuntimeException("Unexpected status: " + datum.getStatus());
                    }
                } else if (value instanceof ParseData) {
                    parseData = (ParseData)value;
                } else if (value instanceof ParseText) {
                    parseText = (ParseText)value;
                } else {
                    logger.warn("Unrecognized type: " + value.getClass());
                }
            }

            if (fetchDatum == null || dbDatum == null || parseText == null || parseData == null) {
                System.out.println("Returning due to null data");
                return;                                     // only have inlinks
            }

            ParseStatus status = parseData.getStatus();
            if (status == null) return;

            String msg = status.getMessage();
            boolean parseError = (msg != null && msg.equals(TIKA_ERROR_STRING));

            if (status.isSuccess() || parseError) {

                String url = decodeUrl(key.toString());
                if (!url.endsWith("/")) {
                    String contentType = getContentTypeFromParseData(parseData);
                    String content = parseError ? null : getUTF8String(parseText.getText());
                    Metadata metadata = parseData.getContentMeta();

                    SolrInputDocument doc = new SolrInputDocument();

                    // add digest, used by dedup
                    doc.addField("digest", metadata.get(Nutch.SIGNATURE_KEY));
                    doc.addField("HDFSKey", key.toString());
                    doc.addField("HDFSSegment", metadata.get(Nutch.SEGMENT_NAME_KEY));
                    doc.addField("boost", 1.0);
                    doc.addField("id", key.toString());
                    doc.addField("url", key.toString());
                    doc.addField("tstamp", solrDateFormat.format(Calendar.getInstance().getTime()));
                    doc.addField("content_type", contentType);
                    doc.addField("content", content);
                    doc = addParseMeta(parseData.getParseMeta(), doc);

                    if (url.startsWith(SMB_PROTOCOL_STRING) &&
                            (doc.getField("last_modified") == null || doc.getField("creation_date") == null)) {
                        doc = addSMBDatesToDoc(doc, url);
                    }

                    if (doc.getField("title") == null) {
                        String[] urlItems = url.split("/");
                        doc.addField("title", urlItems[urlItems.length - 1]);
                    }

                    /*final Parse parse = new ParseImpl(parseText, parseData);
                    NutchDocument nutchDoc = new NutchDocument();
                    nutchDoc.add("segment", metadata.get(Nutch.SEGMENT_NAME_KEY));
                    nutchDoc.add("digest", metadata.get(Nutch.SIGNATURE_KEY));

                    try {
                        // extract information from dbDatum and pass it to
                        // fetchDatum so that indexing filters can use it
                        final Text urlText = (Text) dbDatum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
                        if (urlText != null) {
                            fetchDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY, urlText);
                        }

                        nutchDoc = this.filters.filter(nutchDoc, parse, key, fetchDatum, inlinks);

                    } catch (final IndexingException e) {
                        logger.warn("Error indexing "+key+": "+e);
                        reporter.incrCounter("IndexerStatus", "Errors", 1);
                        return;
                    }

                    // skip documents discarded by indexing filters
                    if (nutchDoc == null) {
                        reporter.incrCounter("IndexerStatus", "Skipped by filters", 1);
                        return;
                    }
                    */
                    try {
                        SERVER.add(doc);
                    } catch (SolrServerException e) {

                        logger.error(e.getMessage());
                    }
                    output.collect(key, doc);
                    reporter.incrCounter("IndexerStatus", "Documents added", 1);
                }
            }
        }
    }

    public static void setMRVariables(String coreName) {
        SOLR_URL      = SOLR_URL_BASE + coreName;
        CORE_PATH     = new Path(CORE_PATH, coreName);
        CRAWL_PATH    = new Path(CORE_PATH, "crawl");
        CRAWLDB_PATH  = new Path(CRAWL_PATH, "crawldb");
        LINKDB_PATH   = new Path(CRAWL_PATH, "linkdb");
        SEGMENTS_PATH = new Path(CRAWL_PATH, "segments");
        System.out.println(SEGMENTS_PATH.toString());
    }

    public static List<Path> getSegments(Path hdfsDirectory) {
        List<Path> filePaths = new ArrayList<Path>();

        try {
            FileSystem fs = FileSystem.get(URI.create(HDFS_URI), new Configuration(), "hdfs");

            for(FileStatus status : fs.listStatus(hdfsDirectory)) {
                filePaths.add(status.getPath());
            }
        } catch (FileNotFoundException e){
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        return filePaths;
    }

    public static void deleteIndex() throws SolrServerException, IOException {
        SERVER.deleteByQuery("*:*");
        SERVER.commit();
    }

    public static void main(String[] args) throws Exception {
        String coreName = args[0];
        setMRVariables(coreName);
        SERVER = new HttpSolrServer(SOLR_URL);
        deleteIndex();

        JobConf conf = new JobConf(SolrUnstructuredIndexer.class);
        conf.setJobName("UnstructuredIndexer");
        conf.set("SOLR_URL", SOLR_URL);
        conf.set("mapred.child.java.opts", "-Xms256m -Xmx1024m -Djava.protocol.handler.pkgs=jcifs");
        conf.set("yarn.nodemanager.resource.memory-gb", "16");
        conf.set("yarn.nodemanager.vmem.to.pmem.limit.ratio", "2.1");
        conf.set("mapreduce.map.memory.mb", "1024");

        conf.set("plugin.folders", "classes/plugins");
        conf.set("plugin.includes", "index-basic");
        conf.set("http.content.limit", "-1");
        conf.set("db.max.outlinks.per.page", "-1");
        conf.set("fetcher.threads.fetch", "20");
        conf.set("fetcher.threads.per.queue", "20");

        //conf.setInputFormat(KeyValueTextInputFormat.class);
        //conf.setInputFormat(TextInputFormat.class);
        conf.setInputFormat(SequenceFileInputFormat.class);

        for (final Path segment : getSegments(SEGMENTS_PATH)) {
            logger.info("IndexerMapReduces: adding segment: " + segment);
            FileInputFormat.addInputPath(conf, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
            FileInputFormat.addInputPath(conf, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
            FileInputFormat.addInputPath(conf, new Path(segment, ParseData.DIR_NAME));
            FileInputFormat.addInputPath(conf, new Path(segment, ParseText.DIR_NAME));
        }

        FileInputFormat.addInputPath(conf, new Path(CRAWLDB_PATH, CrawlDb.CURRENT_NAME));
        FileInputFormat.addInputPath(conf, new Path(LINKDB_PATH, LinkDb.CURRENT_NAME));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setOutputKeyClass(Text.class);
        //conf.setOutputValueClass(Text.class);
        conf.setOutputValueClass(NutchWritable.class);
        //conf.setMapOutputKeyClass(Text.class);
        //conf.setMapOutputValueClass(Text.class);
        conf.setMapOutputValueClass(NutchWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setOutputFormat(TextOutputFormat.class);
        //conf.setOutputFormat(IndexerOutputFormat.class);

        JobClient.runJob(conf);

        SERVER.commit();
    }
}
