import jcifs.smb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.DateField;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WindowsShareCrawler extends NtlmAuthenticator {

    //public static final String SOLR_URL_BASE    = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String HDFS_URI         = "hdfs://denlx006.dn.gates.com:8020";
    //public static final String SMB_URI          = "smb://dnmsfp1.na.corp/roota/";

    public static final String CORE_NAME_KEY            = "core.name";
    public static final String SMB_DOMAIN_KEY           = "smb.domain";
    public static final String SMB_USERNAME_KEY         = "smb.username";
    public static final String SMB_PASSWORD_KEY         = "smb.password";
    public static final String SMB_SERVERNAME_KEY       = "smb.servername";
    public static final String SMB_STARTDIRS_KEY        = "smb.starting_dirs";
    public static final String HDFS_USER_PATH_KEY       = "hdfs.user.path";
    public static final String HDFS_CORE_PATH_KEY       = "hdfs.core.path";
    public static final String SOLR_URL_BASE_KEY        = "solr.url.base";
    public static final String SOLR_URL_KEY             = "solr.url";

    //public static final Path HDFS_USER_PATH             = new Path("/user/hdfs");

    public static HashMap<String, String> properties = new HashMap<String, String>();
    public static HashMap<String, String> confProperties = new HashMap<String, String>();

    public static SolrServer solrServer;
    public static String solrUrl;
    public static String coreName;
    public static Path hdfsCorePath;
    public static List<String> smbStartingDirs = new ArrayList<String>();
    public static NtlmPasswordAuthentication auth;

    private static final Logger logger = Logger.getLogger(WindowsShareCrawler.class);

    public static class Map extends Configured implements Mapper<Text, Text, Text, Text> {

        public void configure(JobConf job) {
            setConf(job);
        }

        public void close() throws IOException { }

        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            output.collect(key, value);
        }
    }

    public static class Reduce extends Configured implements Reducer<Text, Text, Text, SolrInputDocument> {
        public static final String LAST_SAVE_DATE_SOLR_FIELD_NAME       = "last_save_date";
        public static final String LAST_MODIFIED_SOLR_FIELD_NAME        = "last_modified";
        public static final String CREATION_DATE_SOLR_FIELD_NAME        = "creation_date";
        public static final String TITLE_SOLR_FIELD_NAME                = "title";
        public static final String CONTENT_SOLR_FIELD_NAME              = "content";
        public static final String CONTENT_TYPE_SOLR_FIELD_NAME         = "content_type";
        public static final String METADATA_SOLR_FIELD_NAME             = "metadata";
        public static final String BOOST_SOLR_FIELD_NAME                = "boost";
        public static final String ID_SOLR_FIELD_NAME                   = "id";
        public static final String URL_SOLR_FIELD_NAME                  = "url";
        public static final String TSTAMP_SOLR_FIELD_NAME               = "tstamp";

        public static final String HDFS_CRAWLED_DIR_NAME                = "crawled";

        public static SolrServer solrServer;
        public static SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
        public Pattern solrDateString = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})(\\.*)(\\d*)(Z*)");
        public static NtlmPasswordAuthentication auth;
        public static Path hdfsCorePath;
        public static FileSystem fs;
        public static String smbServerName;

        public void configure(JobConf job) {
            setConf(job);

            auth = new NtlmPasswordAuthentication(job.get(SMB_DOMAIN_KEY), job.get(SMB_USERNAME_KEY),
                    job.get(SMB_PASSWORD_KEY));
            hdfsCorePath = new Path(job.get(HDFS_CORE_PATH_KEY), HDFS_CRAWLED_DIR_NAME);
            solrServer = new HttpSolrServer(job.get(SOLR_URL_KEY));
            fs = getHDFSFileSystem();
            smbServerName = job.get(SMB_SERVERNAME_KEY);
        }

        public String getSolrFieldNameFromMetadataName(String metadataName) {
            return metadataName.toLowerCase().replaceAll("-", "_");
        }

        public String checkDateValue(String value) {
            Matcher m = solrDateString.matcher(value);
            if (m.matches()) {
                String newValue = m.group(1) + ".";
                newValue += (m.group(3).equals("") ? "000" : m.group(3)) + "Z";
                return newValue;
            }
            return null;
        }

        public SolrInputDocument setFieldValue(SolrInputDocument doc, String fieldName, String value,
                                               boolean overrideExistingValue) {
            fieldName = getSolrFieldNameFromMetadataName(fieldName);

            if (fieldName.equals(LAST_SAVE_DATE_SOLR_FIELD_NAME) || fieldName.equals(CREATION_DATE_SOLR_FIELD_NAME) ||
                    fieldName.equals(LAST_MODIFIED_SOLR_FIELD_NAME)) {

                String newDateValue = checkDateValue(value);
                if (newDateValue == null) {
                    fieldName += "_string";
                } else {
                    value = newDateValue;
                }
            }

            if (doc.getField(fieldName) == null) {
                doc.addField(fieldName, value);
            } else if (overrideExistingValue) {
                doc.setField(fieldName, value);
            }

            return doc;
        }

        public void close() throws IOException { }


        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, SolrInputDocument> output,
                           Reporter reporter) throws IOException {

            String fileName = key.toString();
            String hdfsFilePath = fileName.replaceAll(smbServerName, "");

            SmbFile f = new SmbFile(fileName, auth);
            long size = f.getContentLength();
            InputStream is = f.getInputStream();

            Path dstPath = new Path(hdfsCorePath, hdfsFilePath);
            FSDataOutputStream out = fs.create(dstPath);

            int len = (int)size;
            byte[] content = new byte[len];
            int n, offset = 0;
            while (offset < size && (n = is.read(content, offset, len-offset)) >= 0){
                offset += n;
                out.write(content);
                //filesAdded += fileName + " " + getMimeType(f.getPath(), content) + "\n";
            }
            is.close();
            out.flush();
            out.close();

            SolrInputDocument doc = createSolrDoc(hdfsFilePath, f, content);
            output.collect(key, doc);

            try {
                solrServer.add(doc);
            } catch (SolrServerException e) {
                System.err.println(e.getMessage());
            }
        }

        private SolrInputDocument createSolrDoc(String key, SmbFile f, byte[] content) {
            HashMap<String, Object> parsedInfo = getParsedInfo(content);
            String parsedText = (String) parsedInfo.get(CONTENT_SOLR_FIELD_NAME);
            Metadata metadata = (Metadata) parsedInfo.get(METADATA_SOLR_FIELD_NAME);

            MediaType contentType = getMimeType(f.getPath(), content);

            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(BOOST_SOLR_FIELD_NAME, 1.0);
            doc.addField(ID_SOLR_FIELD_NAME, key);
            doc.addField(URL_SOLR_FIELD_NAME, key);
            doc.addField(TSTAMP_SOLR_FIELD_NAME, solrDateFormat.format(Calendar.getInstance().getTime()));
            doc.addField(CONTENT_TYPE_SOLR_FIELD_NAME, contentType);
            doc.addField(CONTENT_SOLR_FIELD_NAME, parsedText);

            try {
                String lastModified = DateField.formatExternal(new Date(f.lastModified()));
                Matcher m = solrDateString.matcher(lastModified);
                if (m.matches()) {
                    doc.addField(LAST_MODIFIED_SOLR_FIELD_NAME, lastModified);
                }
                String createTime = DateField.formatExternal(new Date(f.createTime()));
                m = solrDateString.matcher(createTime);
                if (m.matches()) {
                    doc.addField(CREATION_DATE_SOLR_FIELD_NAME, createTime);
                }
            } catch (SmbException e) {
                System.err.println(e.getMessage());
            }

            for (String name : Arrays.asList(metadata.names())) {
                doc = setFieldValue(doc, name, metadata.get(name), true);
            }

            doc = setFieldValue(doc, TITLE_SOLR_FIELD_NAME, f.getName(), false);
            return doc;
        }

        public FileSystem getHDFSFileSystem() {
            try {
                return FileSystem.get(URI.create(HDFS_URI), new Configuration(), "hdfs");
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
            return null;
        }

        public static String getUTF8String(String s) {
            try {
                return new String(s.getBytes(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                return new String(s.getBytes());
            }
        }

        private HashMap<String, Object> getParsedInfo(byte[] content) {
            OutputStream outputstream = new ByteArrayOutputStream();
            ParseContext context = new ParseContext();
            Detector detector = new DefaultDetector();
            Parser parser = new AutoDetectParser(detector);

            Metadata metadata = new Metadata();
            context.set(Parser.class, parser);

            InputStream input = TikaInputStream.get(content, metadata);
            ContentHandler handler = new BodyContentHandler(outputstream);
            try {
                parser.parse(input, handler, metadata, context);
                input.close();
            } catch (SAXException e) {
                System.err.println(e.getMessage());
            } catch (TikaException e) {
                System.err.println(e.getMessage());
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }

            HashMap<String, Object> parsedInfo = new HashMap<String, Object>();
            parsedInfo.put(CONTENT_SOLR_FIELD_NAME, getUTF8String(outputstream.toString()));
            parsedInfo.put(METADATA_SOLR_FIELD_NAME, metadata);
            return parsedInfo;
        }

        private MediaType getMimeType(String fileName, byte[] content) {
            TikaConfig config = TikaConfig.getDefaultConfig();
            Detector detector = config.getDetector();
            TikaInputStream stream = TikaInputStream.get(content);
            Metadata metadata = new Metadata();
            metadata.add(Metadata.RESOURCE_NAME_KEY, fileName);
            try {
                return detector.detect(stream, metadata);
            } catch (IOException e) {
                return MediaType.TEXT_PLAIN;
            }
        }
    }

    public static void setMRVariables(String[] args) {
        String propertiesFile = args[0];
        String line;
        Pattern namePattern = Pattern.compile("\\s*<name>(.*)<\\/name>.*");
        Pattern valPattern  = Pattern.compile("\\s*<value>(.*)<\\/value>.*");

        try {
            DataInputStream in = new DataInputStream(new FileInputStream(propertiesFile));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            while ((line = br.readLine()) != null) {
                boolean propStart = line.contains("<property>"), confPropStart = line.contains("<conf_property>");

                if (propStart || confPropStart) {
                    String propName = "", propValue = "";
                    Matcher m = namePattern.matcher((line = br.readLine()));
                    if (m.matches()) {
                        propName = m.group(1);
                    }
                    m = valPattern.matcher((line = br.readLine()));
                    if (m.matches()) {
                        propValue = m.group(1);
                    }
                    if (!propName.equals("")) {
                        if (propStart) {
                            properties.put(propName, propValue);
                        } else {
                            confProperties.put(propName, propValue);
                        }
                    }
                }
            }
            in.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        coreName = properties.get(CORE_NAME_KEY);
        solrUrl = properties.get(SOLR_URL_BASE_KEY) + coreName;
        hdfsCorePath = new Path(properties.get(HDFS_USER_PATH_KEY), coreName);
        smbStartingDirs = Arrays.asList(properties.get(SMB_STARTDIRS_KEY).split(","));
    }

   /* public void listFiles(SmbFile file, JobConf conf) throws SmbException {
        if (file.isDirectory()) {
            FileInputFormat.addInputPath(conf, new Path(file.toString()));
            System.out.println("Adding " + file.toString());
        }
        for(SmbFile f : file.listFiles()) {
            if (f.isDirectory()) {
                listFiles(f, conf);
            }
        }
    } */

    private void list(SmbFile f) throws IOException {
        SmbFile[] children = new SmbFile[0];
        JobConf conf = new JobConf(WindowsShareCrawler.class);
        FileSystem fs = FileSystem.get(conf);

        Path output = new Path(String.valueOf(new Date().getTime()));
        FSDataOutputStream out = fs.create(output);

        if (f.isDirectory()) {
            conf.setJobName("WindowsShareCrawler " + f.getPath());
            conf.set(SMB_DOMAIN_KEY, properties.get(SMB_DOMAIN_KEY));
            conf.set(SMB_PASSWORD_KEY, properties.get(SMB_PASSWORD_KEY));
            conf.set(SMB_USERNAME_KEY, properties.get(SMB_USERNAME_KEY));
            conf.set(SMB_SERVERNAME_KEY, properties.get(SMB_SERVERNAME_KEY));
            conf.set(HDFS_CORE_PATH_KEY, hdfsCorePath.toString());
            conf.set(SOLR_URL_KEY, solrUrl);

            for(java.util.Map.Entry<String,String> entry : confProperties.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
            /*
            conf.set("mapred.child.java.opts", "-Xms512m -Xmx800m -Djava.protocol.handler.pkgs=jcifs");
            conf.set("yarn.nodemanager.resource.memory-gb", "64");
            conf.set("yarn.nodemanager.vmem.to.pmem.limit.ratio", "4.1");
            conf.set("mapreduce.map.memory.mb", "4096");
            conf.set("dfs.datanode.max.transfer.threads", "1024");

            conf.set("plugin.folders", "classes/plugins");
            conf.set("plugin.includes", "index-basic");
            conf.set("http.content.limit", "-1");
            conf.set("db.max.outlinks.per.page", "-1");
            conf.set("fetcher.threads.fetch", "20");
            conf.set("fetcher.threads.per.queue", "20");
            */
            conf.setJarByClass(WindowsShareCrawler.class);
            conf.setInputFormat(KeyValueTextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            try {
                children = f.listFiles();
                for(SmbFile file : children) {
                    String name = file.getName();
                    if (!file.isDirectory() && !name.endsWith(".lnk") && !name.endsWith(".zip")) {
                        out.write(file.getPath().getBytes());
                        out.write("\n".getBytes());
                    }
                }
            } catch (SmbException e) {
                e.printStackTrace();
            }
            out.flush();
            out.close();

            Path outputPath = new Path(hdfsCorePath, "test/test_" + String.valueOf(new Date().getTime()));
            FileInputFormat.setInputPaths(conf, output.getName());
            FileOutputFormat.setOutputPath(conf, outputPath);

            try {
                FsStatus fsStatus = fs.getStatus();
                while(fsStatus.getRemaining() < 5368709120L) {
                    Thread.sleep(20000);
                    fsStatus = fs.getStatus();
                }
                JobClient.runJob(conf);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for(Path p : Arrays.asList(output, outputPath)) {
                fs.delete(p, true);
            }
            try {
                solrServer.commit();
            } catch (SolrServerException e) {
                System.err.println(e.getMessage());
            }
            for(SmbFile child : children) {
                list(child);
            }
        }
    }

    public static void deleteIndex() throws SolrServerException, IOException {
        solrServer.deleteByQuery("*:*");
        solrServer.commit();
    }

    public WindowsShareCrawler() throws Exception {

        NtlmAuthenticator.setDefault(this);
        solrServer = new HttpSolrServer(solrUrl);
        deleteIndex();

        for(String smbDir : smbStartingDirs) {
            list(new SmbFile(smbDir, auth));
        }
    }

    protected NtlmPasswordAuthentication getNtlmPasswordAuthentication() {
        return new NtlmPasswordAuthentication(properties.get(SMB_DOMAIN_KEY),
                properties.get(SMB_USERNAME_KEY), properties.get(SMB_PASSWORD_KEY));
    }

    public static void main(String[] args) throws Exception {
        setMRVariables(args);

        new WindowsShareCrawler();
    }
}
