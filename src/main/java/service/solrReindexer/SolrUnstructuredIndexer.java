import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
/*import com.zehon.FileTransferStatus;
import com.zehon.exception.FileTransferException;
import com.zehon.sftp.SFTP;*/
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
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolrUnstructuredIndexer {

    public static final String SOLR_URL_BASE         = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String SOLR_THUMBNAILS_URI   = SOLR_URL_BASE + "thumbnails";
    public static final String HDFS_URI              = "hdfs://denlx006.dn.gates.com:8020";
    public static final String CONVERT_URL           = "http://denlx014.dn.gates.com:18680/convert2swf";
    public static final String TMP_DIRECTORY         = "/tmp/prizm/";
    public static final String FILE_EXTENSION_PNG    = "png";
    public static final String FILE_EXTENSION_PDF    = "pdf";
    public static final Path USER_HDFS_PATH          = new Path("/user/hdfs");

    public static final String LOCALHOST_STRING      = "http://localhost/";
    public static final String JOB_MAP_KEY_LOCALHOST = "LOCALHOST_MAPPING";
    public static final String JOB_MAP_KEY_SOLRURL   = "SOLR_URL";
    public static final String JOB_MAP_KEY_CORENAME  = "CORE_NAME";
    public static final String JOB_MAP_KEY_LOGWRITER = "LOG_WRITER";

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
    public static final String LOG_FILE              = "/tmp/prizm/log";

    public static File logFile = new File(LOG_FILE);
    public static BufferedWriter logWriter;
    public static SolrServer thumbnailSolrServer;
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
        public static SolrServer thumbnailSolrServer;
        public static String solrUrl;
        public static String localhostMapping;
        public static String coreName;
        public static BufferedWriter logWriter;
        public static SimpleDateFormat solrDateFormat  = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
        public static Pattern solrDateString = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})(\\.*)(\\d*)(Z*)");
        public static final String PRIZM_HOST     = "denlx014.dn.gates.com";
        public static final String PRIZM_USERNAME = "cm0607";
        public static final String PRIZM_PASSWORD = "9pigiyac";
        public static final String PRIZM_FOLDER   = "/tmp/prizm/";
        public static final String TIKA_ERROR_STRING   = "Can't retrieve Tika parser for mime-type application/octet-stream";

        public static final String LAST_MODIFIED_FIELD      = "last_modified";
        public static final String CREATION_DATE_FIELD      = "creation_date";
        public static final String APPLICATION_NAME_FIELD   = "application_name";
        public static final String LAST_AUTHOR_FIELD        = "last_author";
        public static final String AUTHOR_FIELD             = "author";
        public static final String COMPANY_FIELD            = "company";
        public static final String TITLE_FIELD              = "title";

        public static void writeToLog(String s) {
            try {
                logWriter.write(s + "\n");
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

        private NtlmPasswordAuthentication auth;

        HashMap<String, String> mapMetadataToSolrFields = new HashMap<String, String>() {{
            put("Last-Save-Date", LAST_MODIFIED_FIELD);
            put("Last-Modified", LAST_MODIFIED_FIELD);
            put("Creation-Date", CREATION_DATE_FIELD);
            put("Last-Author", LAST_AUTHOR_FIELD);
            put("Application-Name", APPLICATION_NAME_FIELD);
            put("Author", AUTHOR_FIELD);
            put("Company", COMPANY_FIELD);
            put(TITLE_FIELD, TITLE_FIELD);
        }};

        public void configure(JobConf job) {
            setConf(job);
            solrUrl          = job.get(JOB_MAP_KEY_SOLRURL);
            localhostMapping = job.get(JOB_MAP_KEY_LOCALHOST);
            coreName         = job.get(JOB_MAP_KEY_CORENAME);
            solrServer       = new HttpSolrServer(solrUrl);
            thumbnailSolrServer = new HttpSolrServer(SOLR_THUMBNAILS_URI);

            try {
                logWriter = new BufferedWriter(new FileWriter(logFile, true));
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
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
                url = url.replaceAll("\\+", "%2B");
                writeToLog("Decoding " + url);
                url = URLDecoder.decode(url, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                writeToLog("Can't decode! " + url);
            } catch (IllegalArgumentException e) {
                writeToLog("Can't decode! " + url);
            }
            return url;
        }

        public String getFullSolrDate(String value) {
            Matcher m = solrDateString.matcher(value);
            if (m.matches()) {
                String newValue = m.group(1) + ".";
                newValue += (m.group(3).equals("") ? "000" : m.group(3)) + "Z";
                return newValue;
            }
            return null;
        }

        public static String getContentTypeFromParseData(ParseData parseData){
            String contentType = parseData.getContentMeta().get(HttpHeaders.CONTENT_TYPE);
            if (contentType == null || contentType.equals("")) {
                contentType = parseData.getParseMeta().get(HttpHeaders.CONTENT_TYPE);
            }
            return (contentType == null) ? "" : contentType;
        }

        private SolrInputDocument addParseMeta(Metadata metadata, SolrInputDocument doc) {
            if (metadata != null) {
                List<String> names = Arrays.asList(metadata.names());
                for (java.util.Map.Entry<String,String> entry : mapMetadataToSolrFields.entrySet()) {
                    if (names.contains(entry.getKey())) {
                        String solrField = entry.getValue();
                        String metadataVal = metadata.get(entry.getKey());

                        if (solrField.equals(LAST_MODIFIED_FIELD) || solrField.equals(CREATION_DATE_FIELD)) {
                            String newdate = getFullSolrDate(metadataVal);
                            if (newdate == null) {
                                solrField = entry.getKey();
                            } else {
                                metadataVal = newdate;
                            }
                        }
                        doc.addField(solrField, metadataVal);
                    }
                }
            }

            return doc;
        }

        private NtlmPasswordAuthentication getAuth() {
            if (this.auth == null) {
                this.auth = new NtlmPasswordAuthentication("NA", "bigdatasvc", "Crawl2013");
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

        public String getTempDocName(String path) {
            return path.replaceAll(LOCALHOST_STRING, "").replaceAll("/", "_").replaceAll(" |\\$|~", "");
        }

        public String changeExtension(String f, String newExt) {
            return (f.contains(".") ? f.substring(0, f.lastIndexOf(".")) : f) + "." + newExt;
        }

        public boolean removeLocalFile(File f) {
            return f.exists() && f.isFile() && f.delete();
        }

        private void cleanupTempDir(File localFile) {
            String f = localFile.getName();
            removeLocalFile(localFile);
            removeLocalFile(new File(changeExtension(f, FILE_EXTENSION_PDF)));
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

            writeToLog(sb.toString());
        }

        public void httpGetRequest(String url) throws IOException {
            DefaultHttpClient httpclient = new DefaultHttpClient();
            try {
                HttpGet httpget = new HttpGet(url);
                HttpResponse response = httpclient.execute(httpget);
                getResponse(response.getEntity());
            } catch (IllegalArgumentException e) {
                writeToLog("GET request failed: " + url + ", " + e.getMessage());
            }
        }

        public boolean fileTransfer(String fileName, String filePath, boolean upload) {
            int status;
            boolean success = false;
            /*
            try {
                status = upload ? SFTP.sendFile(filePath, PRIZM_FOLDER, PRIZM_HOST, PRIZM_USERNAME, PRIZM_PASSWORD) :
                                  SFTP.getFile(fileName, PRIZM_FOLDER, PRIZM_HOST, PRIZM_USERNAME, PRIZM_PASSWORD, PRIZM_FOLDER);
                success = (FileTransferStatus.SUCCESS == status);

                if (upload && success) {
                    int i = 0;
                    while (i < 10 && !SFTP.fileExists(PRIZM_FOLDER, fileName, PRIZM_HOST, PRIZM_USERNAME, PRIZM_PASSWORD)) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            System.err.println(e.getMessage());
                        }
                        i++;
                    }
                }
            } catch (FileTransferException e) {
                e.printStackTrace();
            }

            writeToLog("SFTP " + (upload ? "upload " : "download ") + filePath + " success : " + success); */
            return success;
        }

        public String convertContentToThumbnail(byte[] content, String uri) {

            if (!uri.endsWith("/")) {
                String fileName  = getTempDocName(uri);
                String thumbName = changeExtension(fileName, FILE_EXTENSION_PNG);
                File localFile   = new File(TMP_DIRECTORY, fileName);
                File localThumb  = new File(TMP_DIRECTORY, thumbName);

                try {
                    if (writeLocalFile(localFile, content)) {

                        fileTransfer(fileName, localFile.getPath(), true);
                        String getRequest = CONVERT_URL + "?source=" + localFile.getPath() + "&target=" + localThumb.getPath() + "&pages=1";
                        writeToLog("GET: " + getRequest);
                        httpGetRequest(getRequest);
                        fileTransfer(thumbName, localThumb.getPath(), false);
                        cleanupTempDir(localFile);

                        if (localThumb.exists()) {
                            BufferedImage image = ImageIO.read(localThumb);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            ImageIO.write(image, FILE_EXTENSION_PNG, baos);
                            return new String(Base64.encodeBase64(baos.toByteArray()));
                        } else {
                            writeToLog("Thumbnail transfer not successful! " + localThumb.getPath());
                        }
                    }
                } catch (IOException e) {
                    writeToLog(e.getMessage());
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

            String digest = metadata.get(Nutch.SIGNATURE_KEY);
            doc.addField(SOLR_KEY_DIGEST,       digest == null ? "" : digest);
            doc.addField(SOLR_KEY_HDFSKEY,      key);
            doc.addField(SOLR_KEY_HDFSSEG,      metadata.get(Nutch.SEGMENT_NAME_KEY));
            doc.addField(SOLR_KEY_BOOST,        1.0);
            doc.addField(SOLR_KEY_ID,           key);
            doc.addField(SOLR_KEY_URL,          key);
            doc.addField(SOLR_KEY_TSTAMP,       solrDateFormat.format(Calendar.getInstance().getTime()));
            doc.addField(SOLR_KEY_CONTENT_TYPE, contentType);
            doc.addField(SOLR_KEY_CONTENT,      content);
            if (parseData != null) {
                doc = addParseMeta(parseData.getParseMeta(), doc);
            }

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

            CrawlDatum dbDatum      = null;
            CrawlDatum fetchDatum   = null;
            ParseData parseData     = null;
            ParseText parseText     = null;
            Content content         = null;

            String url = key.toString();
            if (url.endsWith("/") || url.endsWith("%2F")) {
                return;
            }

            writeToLog("Reducing key " + key.toString());
            while (values.hasNext()) {
                final Writable value = values.next().get(); // unwrap
                if (value instanceof CrawlDatum) {
                    final CrawlDatum datum = (CrawlDatum)value;
                    int datumStatus = datum.getStatus();
                    if (CrawlDatum.hasDbStatus(datum)) {
                        dbDatum = datum;
                    } else if (CrawlDatum.hasFetchStatus(datum)) {
                        fetchDatum = datum;

                    } else if (datumStatus == CrawlDatum.STATUS_LINKED || datumStatus == CrawlDatum.STATUS_SIGNATURE ||
                                datumStatus == CrawlDatum.STATUS_PARSE_META) {
                        continue;
                    } else {
                        throw new RuntimeException("Unexpected status: " + datum.getStatus());
                    }
                } else if (value instanceof ParseData) {
                    parseData = (ParseData)value;
                } else if (value instanceof ParseText) {
                    parseText = (ParseText)value;
                } else if (value instanceof Content) {
                    content = (Content) value;
                } else {
                    writeToLog("Unrecognized type: " + value.getClass() + " for key " + key.toString());
                }
            }

            if (content == null) {
                return;
            }

            url = decodeUrl(key.toString());
            if (fetchDatum == null || dbDatum == null || parseText == null || parseData == null) {
                String n = "null {" + (fetchDatum == null ? "fetchDatum," : "") + (dbDatum == null ? "dbDatum," : "") +
                        (parseData == null ? "parseData," : "") + (parseText == null ? "parseText" : "") + "}";

                writeToLog("**Just adding content** " + key.toString() + " due to " + n);
            }


            String contentType  = parseData != null ? getContentTypeFromParseData(parseData) : content.getContentType();
            byte[] contentBytes = parseText != null ? parseText.getText().getBytes() : content.getContent();
            String contentStr   = parseText != null ? getUTF8String(parseText.getText()) : new String(contentBytes);
            Metadata metadata   = parseData != null ? parseData.getContentMeta() : content.getMetadata();

            writeToLog("Adding " + url);
            SolrInputDocument doc = addContentToSolr(metadata, url, contentType, contentStr, parseData);
            if (contentBytes != null && contentBytes.length > 0) {
                writeToLog("Generating thumbnail " + url);
                addThumbnail(contentBytes, url);
            } else {
                writeToLog("No content for thumbnail " + url);
            }
            if (doc != null) {
                output.collect(key, doc);
                reporter.incrCounter("IndexerStatus", "Documents added", 1);
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

    public static void deleteIndex(SolrServer server) throws SolrServerException, IOException {
        server.deleteByQuery("*:*");
        server.commit();
    }

    public static void main(String[] args) throws Exception {
        setMRVariables(args);
        solrServer = new HttpSolrServer(solrUrl);
        thumbnailSolrServer = new HttpSolrServer(SOLR_THUMBNAILS_URI);
        deleteIndex(solrServer);
        deleteIndex(thumbnailSolrServer);

        logWriter = new BufferedWriter(new FileWriter(logFile, true));

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
            FileInputFormat.addInputPath(conf, new Path(segment, Content.DIR_NAME));
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
        logWriter.close();
    }
}
