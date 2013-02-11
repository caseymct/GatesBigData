import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
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

    public static final String SOLR_URL_BASE         = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String SOLR_THUMBNAILS_URI   = SOLR_URL_BASE + "thumbnails";
    public static final String HDFS_URI              = "hdfs://denlx006.dn.gates.com:8020";
    public static final String CONVERT_URL           = "http://denlx014.dn.gates.com:18680/convert2swf";
    public static final String TMP_DIRECTORY         = "/tmp/prizm/";
    public static final Path USER_HDFS_PATH          = new Path("/user/hdfs");

    public static final String LOCALHOST_STRING      = "http://localhost/";
    public static final String JOB_MAP_KEY_LOCALHOST = "LOCALHOST_MAPPING";
    public static final String JOB_MAP_KEY_SOLRURL   = "SOLR_URL";
    public static final String JOB_MAP_KEY_CORENAME  = "CORE_NAME";

    public static final String SOLR_KEY_DIGEST       = "digest";
    public static final String SOLR_KEY_HDFSKEY      = "HDFSKey";
    public static final String SOLR_KEY_HDFSSEG      = "HDFSSegment";
    public static final String SOLR_KEY_BOOST        = "boost";
    public static final String SOLR_KEY_ID           = "id";
    public static final String SOLR_KEY_URL          = "url";
    public static final String SOLR_KEY_TSTAMP       = "tstamp";
    public static final String SOLR_KEY_CONTENT_TYPE = "content_type";
    public static final String SOLR_KEY_CONTENT      = "content";
    public static final String SOLR_KEY_TITLE        = "title";
    public static final String SOLR_KEY_CORENAME     = "core";
    public static final String SOLR_KEY_THUMBNAIL    = "thumbnail";

    public static SolrServer thumbnailSolrServer = new HttpSolrServer(SOLR_THUMBNAILS_URI);
    public static SolrServer solrServer;
    public static String solrUrl          = "";
    public static String coreName         = "";
    public static String outputPath       = "";
    public static String localhostMapping = "";

    public static Path corePath     = null;
    public static Path crawlPath    = null;
    public static Path segmentsPath = null;
    public static Path crawldbPath  = null;
    public static Path linkdbPath   = null;

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
        public static SolrServer solrServer;
        public static String solrUrl;
        public static String localhostMapping;
        public static String coreName;

        public static SimpleDateFormat solrDateFormat  = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
        public static final String SMB_PROTOCOL_STRING = "smb://";
        public static final String SMB_DOMAIN          = "NA";
        public static final String SMB_USERNAME        = "bigdatasvc";
        public static final String SMB_PASSWORD        = "Crawl2012";
        public static final String TIKA_ERROR_STRING   = "Can't retrieve Tika parser for mime-type application/octet-stream";

        public static final String LAST_MODIFIED_FIELD = "last_modified";

        private NtlmPasswordAuthentication auth;

        HashMap<String, String> mapMetadataToSolrFields = new HashMap<String, String>() {{
            put("Last-Save-Date", LAST_MODIFIED_FIELD);
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
            solrUrl          = job.get(JOB_MAP_KEY_SOLRURL);
            localhostMapping = job.get(JOB_MAP_KEY_LOCALHOST);
            coreName         = job.get(JOB_MAP_KEY_CORENAME);
            solrServer       = new HttpSolrServer(solrUrl);
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
                System.out.println("Decoding " + url);
                url = URLDecoder.decode(url, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                System.err.println("Could not decode URL: " + e.getMessage());
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
                    doc = addFieldIfNull(doc, LAST_MODIFIED_FIELD, solrDateFormat.format(new Date(file.getLastModified())));
                    doc = addFieldIfNull(doc, "creation_date", solrDateFormat.format(new Date(file.createTime())));
                }
            } catch (SmbException e) {
                System.err.println(e.getMessage());
            } catch (MalformedURLException e){
                System.err.println(e.getMessage());
            }
            return doc;
        }

        private SolrInputDocument addDatesToDoc(SolrInputDocument doc, String url) {
            File file = new File(url.replace(LOCALHOST_STRING, localhostMapping));
            if (file.exists()) {
                doc = addFieldIfNull(doc, LAST_MODIFIED_FIELD, solrDateFormat.format(new Date(file.lastModified())));
            }
            return doc;
        }

        public String getTempDocNameFromHDFSId(String path) {
            return path.replaceAll(LOCALHOST_STRING, "").replaceAll("/", "_").replaceAll(" |\\$|~", "");
        }

        public String getThumbnailNameFromHDFSPath(String f) {
            return f.substring(0, f.lastIndexOf(".")) + ".png";
        }

        public boolean removeLocalFile(File f) {
            return f.exists() && f.isFile() && f.delete();
        }

        private void cleanupTempDir(File localFile) {
            String f = localFile.getName();
            String pdfFileName = f.substring(0, f.lastIndexOf(".")) + ".pdf";
            removeLocalFile(localFile);
            removeLocalFile(new File(TMP_DIRECTORY, pdfFileName));
        }

        public boolean writeLocalFile(File f, byte[] content) throws IOException {
            if (f.exists() && !f.canWrite()) {
                return false;
            }

            FileOutputStream fos = new FileOutputStream(f);
            fos.write(content);
            fos.flush();
            fos.close();

            return f.exists();
        }

        private void getResponse(HttpEntity entity) throws IOException {
            StringBuilder sb = new StringBuilder();
            String line;

            if (entity != null) {
                InputStream inputStream = entity.getContent();
                BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

                while((line = in.readLine()) != null) {
                    sb.append(line);
                }
                inputStream.close();
            }

            System.out.println(sb.toString());
        }

        public void httpGetRequest(String url) throws IOException {
            DefaultHttpClient httpclient = new DefaultHttpClient();
            HttpGet httpget = new HttpGet(url);
            HttpResponse response = httpclient.execute(httpget);

            getResponse(response.getEntity());
        }

        public String convertContentToThumbnail(byte[] content, String uri) {
            //String uri = decodeUrl(url);

            if (!uri.endsWith("/")) {
                String fileName = getTempDocNameFromHDFSId(uri);
                File localThumb = new File(TMP_DIRECTORY, "test.png");//getThumbnailNameFromHDFSPath(fileName));
                System.out.println("File " + localThumb.getPath() + " exists : " + localThumb.exists());
                File localFile  = new File(TMP_DIRECTORY, fileName);



                try {
                    if (writeLocalFile(localFile, content)) {
                        //Runtime.getRuntime().exec("java -jar convert2swfclient.jar source=" + localFile.getPath() + "target=" + localThumb.getPath() + "pages=1");
                        /*
                        String getRequest = CONVERT_URL + "?source=" + localFile.getPath() + "&target=" + localThumb.getPath() + "&pages=1";
                        System.out.println(getRequest);
                        httpGetRequest(getRequest);
                        */
                        cleanupTempDir(localFile);

                        if (localThumb.exists()) {
                            return Base64.encodeBase64String(FileUtils.readFileToByteArray(localThumb));
                        } else {
                            System.out.println("Thumbnail conversion not successful! " + localThumb.getPath());
                        }
                    }
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
            return "";
        }

        public void addThumbnail(byte[] content, String url) throws IOException {
            String imgContent = convertContentToThumbnail(content, url);
            if (imgContent.equals("")) return;

            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(SOLR_KEY_ID, url);
            doc.addField(SOLR_KEY_THUMBNAIL, imgContent);
            doc.addField(SOLR_KEY_CORENAME, coreName);

            try {
                thumbnailSolrServer.add(doc);
            } catch (SolrServerException e) {
                System.out.println(e.getMessage());
            }
        }

        public SolrInputDocument addContentToSolr(Metadata metadata, String key, String contentType, String content,
                                     ParseData parseData) throws IOException {
            SolrInputDocument doc = new SolrInputDocument();

            doc.addField(SOLR_KEY_DIGEST,       metadata.get(Nutch.SIGNATURE_KEY));
            doc.addField(SOLR_KEY_HDFSKEY,      key);
            doc.addField(SOLR_KEY_HDFSSEG,      metadata.get(Nutch.SEGMENT_NAME_KEY));
            doc.addField(SOLR_KEY_BOOST,        1.0);
            doc.addField(SOLR_KEY_ID,           key);
            doc.addField(SOLR_KEY_URL,          key);
            doc.addField(SOLR_KEY_TSTAMP,       solrDateFormat.format(Calendar.getInstance().getTime()));
            doc.addField(SOLR_KEY_CONTENT_TYPE, contentType);
            doc.addField(SOLR_KEY_CONTENT,      content);
            doc = addParseMeta(parseData.getParseMeta(), doc);

            //if (url.startsWith(SMB_PROTOCOL_STRING) && (doc.getField(LAST_MODIFIED_FIELD) == null || doc.getField("creation_date") == null))
            if (key.startsWith(LOCALHOST_STRING) && doc.getField(LAST_MODIFIED_FIELD) == null) {
                doc = addDatesToDoc(doc, key);
            }

            if (doc.getField(SOLR_KEY_TITLE) == null) {
                String[] urlItems = key.split("/");
                doc.addField(SOLR_KEY_TITLE, urlItems[urlItems.length - 1]);
            }

            try {
                solrServer.add(doc);
                return doc;
            } catch (SolrServerException e) {
                System.err.println(e.getMessage());
            }
            return null;
        }

        public void reduce(Text key, Iterator<NutchWritable> values, OutputCollector<Text, SolrInputDocument> output,
                           Reporter reporter) throws IOException {

            CrawlDatum dbDatum = null;
            CrawlDatum fetchDatum = null;
            ParseData parseData = null;
            ParseText parseText = null;

            System.out.println("Reducing key " + key.toString());
            while (values.hasNext()) {
                final Writable value = values.next().get(); // unwrap
                if (value instanceof CrawlDatum) {
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
                    System.out.println("Unrecognized type: " + value.getClass());
                }
            }

            if (fetchDatum == null || dbDatum == null || parseText == null || parseData == null) {
                System.out.println("Returning " + key.toString() + " due to null data");
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

                    System.out.println("Adding " + url);
                    SolrInputDocument doc = addContentToSolr(metadata, url, contentType, content, parseData);
                    if (content != null) {
                        addThumbnail(content.getBytes(), url);
                    }
                    if (doc != null) {
                        output.collect(key, doc);
                        reporter.incrCounter("IndexerStatus", "Documents added", 1);
                    }
                }
            }
        }
    }

    public static void setMRVariables(String[] args) {
        coreName    = args[0];
        outputPath  = args[1];

        solrUrl      = SOLR_URL_BASE + coreName;
        corePath     = new Path(USER_HDFS_PATH, coreName);
        crawlPath    = new Path(corePath,  "crawl");
        crawldbPath  = new Path(crawlPath, "crawldb");
        linkdbPath   = new Path(crawlPath, "linkdb");
        segmentsPath = new Path(crawlPath, "segments");
        System.out.println(segmentsPath.toString());

        if (args.length == 4) {
            String fileName = args[3];

            try {
                DataInputStream in = new DataInputStream(new FileInputStream(fileName));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = "";

                while ((line = br.readLine()) != null) {
                    String[] lineComponents = line.split("=");
                    String key = lineComponents[0].toUpperCase();
                    String val = lineComponents[1];
                    if (key.equals(JOB_MAP_KEY_LOCALHOST)) {
                        localhostMapping = val;
                    }
                }
                in.close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    public static List<Path> getSegments(Path hdfsDirectory) {
        List<Path> filePaths = new ArrayList<Path>();

        try {
            FileSystem fs = FileSystem.get(URI.create(HDFS_URI), new Configuration(), "hdfs");

            for(FileStatus status : fs.listStatus(hdfsDirectory)) {
                filePaths.add(status.getPath());
            }
        } catch (FileNotFoundException e){
            System.err.println(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }

        return filePaths;
    }

    public static void deleteIndex() throws SolrServerException, IOException {
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
    }

    public static void main(String[] args) throws Exception {
        setMRVariables(args);
        solrServer = new HttpSolrServer(solrUrl);
        deleteIndex();

        JobConf conf = new JobConf(SolrUnstructuredIndexer.class);
        conf.setJobName("UnstructuredIndexer");
        conf.set(JOB_MAP_KEY_SOLRURL, solrUrl);
        conf.set(JOB_MAP_KEY_LOCALHOST, localhostMapping);
        conf.set(JOB_MAP_KEY_CORENAME, coreName);
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

        conf.setInputFormat(SequenceFileInputFormat.class);

        for (final Path segment : getSegments(segmentsPath)) {
            FileInputFormat.addInputPath(conf, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
            FileInputFormat.addInputPath(conf, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
            FileInputFormat.addInputPath(conf, new Path(segment, ParseData.DIR_NAME));
            FileInputFormat.addInputPath(conf, new Path(segment, ParseText.DIR_NAME));
        }

        FileInputFormat.addInputPath(conf, new Path(crawldbPath, CrawlDb.CURRENT_NAME));
        FileInputFormat.addInputPath(conf, new Path(linkdbPath, LinkDb.CURRENT_NAME));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NutchWritable.class);
        conf.setMapOutputValueClass(NutchWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);

        solrServer.commit();
        thumbnailSolrServer.commit();
    }
}
