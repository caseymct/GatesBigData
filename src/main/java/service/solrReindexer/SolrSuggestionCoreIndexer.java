import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrSuggestionCoreIndexer {

    public static final String DEFAULT_DELIMITER         = ",";
    public static final String DEFAULT_NEWLINE           = System.getProperty("line.separator");

    public static final String SOLR_URL_BASE             = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String SOLR_KEY_ID               = "id";
    public static final String SOLR_KEY_COUNT            = "count";

    public static final String LOCAL_LOG_FILE_DEFAULT    = "/tmp/tsvlog";
    public static final String FACET_FILE_NAME_PREFIX    = "facets";
    public static final String LOCAL_FACET_FILE_PATH     = "./";

    public static final String HDFS_URI                  = "hdfs://denlx006.dn.gates.com:8020";
    public static final String HDFS_USERNAME             = "hdfs";
    public static final String HDFS_USER_PATH            = "/user/hdfs/";

    public static final String CONF_KEY_SUGG_CORE_NAME   = "SUGGESTION_CORE";
    public static final String CONF_KEY_ORIG_CORE_NAME   = "ORIG_CORE";
    public static final String CONF_KEY_DELIMITER        = "DELIMITER";
    public static final String CONF_KEY_LOGFILE          = "LOGFILE";
    public static final String CONF_KEY_SUGG_CORE_FIELDS = "FIELDS";

    public static final String JOB_MAP_KEY_SOLR_URL      = "SOLR_URL";
    public static final String JOB_MAP_KEY_LOG_FILE      = "LOGFILE";

    public static String localLogFilePath;
    public static SolrServer suggCoreSolrServer;
    public static String origCoreName;
    public static String origCoreSolrUrl;
    public static String suggCoreName;
    public static String suggCoreSolrUrl;
    public static String fields;
    public static String hdfsFacetFilePath;
    public static String delimiter = DEFAULT_DELIMITER;
    public static String localFacetFilePath;

    public static class Map extends Configured implements Mapper<LongWritable, Text, Text, Text> {
        public static SolrServer solrServer;
        public static BufferedWriter mapLogWriter;

        public void configure(JobConf job) {
            setConf(job);

            solrServer   = new HttpSolrServer(job.get(JOB_MAP_KEY_SOLR_URL));
            File logFile = new File(job.get(JOB_MAP_KEY_LOG_FILE));
            try {
                mapLogWriter = new BufferedWriter(new FileWriter(logFile, true));
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

        public void close() throws IOException {
            mapLogWriter.close();
        }

        public static void writeToLog(String s) {
            try {
                mapLogWriter.write(s + "\n");
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String valueString = value.toString();

            int firstIndex    = valueString.indexOf(":"), lastIndex = valueString.lastIndexOf(":");
            String fieldName  = valueString.substring(0, firstIndex);
            String fieldValue = valueString.substring(firstIndex + 1, lastIndex);
            long count = Long.parseLong(valueString.substring(lastIndex + 1));

            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.addField(SOLR_KEY_ID, UUID.randomUUID());
            solrDoc.addField(fieldName, fieldValue);
            solrDoc.addField(SOLR_KEY_COUNT, count);

            try {
                solrServer.add(solrDoc);
            } catch (SolrServerException e) {
                output.collect(new Text(e.getMessage()), new Text(""));
                writeToLog("Solr Server exception: " + e.getMessage());
            }

            output.collect(new Text(fieldName), new Text(fieldValue + "(" + count + ")"));
        }
    }

    public static class Reduce extends Configured implements Reducer<Text, Text, Text, Text> {
        public void configure(JobConf job) {
            setConf(job);
        }

        public void close() throws IOException { }

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            output.collect(key, new Text(""));
        }
    }

    public static void deleteIndex(SolrServer server)  {
        try {
            server.deleteByQuery("*:*");
            server.commit();
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void setMRVariables(String fileName) {
        localLogFilePath = LOCAL_LOG_FILE_DEFAULT;

        try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            BufferedReader br  = new BufferedReader(new InputStreamReader(in));
            String line;

            while ((line = br.readLine()) != null) {
                String[] lineComponents = line.split("=");
                String key = lineComponents[0].toUpperCase();
                String val = lineComponents[1];

                if (key.equals(CONF_KEY_ORIG_CORE_NAME)) {
                    origCoreName = val;
                    origCoreSolrUrl = SOLR_URL_BASE + origCoreName;
                } else if (key.equals(CONF_KEY_SUGG_CORE_NAME)) {
                    suggCoreName = val;
                    suggCoreSolrUrl = SOLR_URL_BASE + suggCoreName;
                } else if (key.equals(CONF_KEY_DELIMITER)) {
                    delimiter = val;
                } else if (key.equals(CONF_KEY_SUGG_CORE_FIELDS)) {
                    fields = val;
                } else if (key.equals(CONF_KEY_LOGFILE)) {
                    localLogFilePath = val;
                }
            }
            in.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        String facetFileName = FACET_FILE_NAME_PREFIX + "_" + origCoreName;
        localFacetFilePath   = LOCAL_FACET_FILE_PATH + facetFileName;
        hdfsFacetFilePath    = HDFS_USER_PATH + suggCoreName + "/" + facetFileName;
    }

    public static boolean addFile(String remoteFilePath, String localFilePath) {
        boolean success = false;

        try {
            FileSystem fs = FileSystem.get(URI.create(HDFS_URI), new Configuration(), HDFS_USERNAME);
            Path srcPath = new Path(localFilePath);
            Path dstPath = new Path(remoteFilePath);

            if (fs.exists(dstPath)) {
                fs.delete(dstPath, false);
            }
            fs.copyFromLocalFile(srcPath, dstPath);
            success = fs.exists(dstPath);
            fs.close();

        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

        return success;
    }

    public static boolean writeFacetFile() {
        File f = new File(localFacetFilePath);
        HttpSolrServer solrServer = new HttpSolrServer(origCoreSolrUrl);

        try {
            BufferedWriter localFacetFileWriter = new BufferedWriter(new FileWriter(f, true));
            BufferedWriter logFileWriter = new BufferedWriter(new FileWriter(localLogFilePath, true));

            for(String field : fields.split(delimiter)) {
                System.out.println("Field " + field + ", delim " + delimiter);
                SolrQuery query = new SolrQuery("*:*");

                query.setStart(0);
                query.setRows(0);
                query.setFacet(true);
                query.setFacetSort(SOLR_KEY_COUNT);
                query.setFacetLimit(-1);
                query.addFacetField(field);

                QueryResponse rsp = solrServer.query(query);
                List<FacetField> facetFieldList = rsp.getFacetFields();
                if (facetFieldList == null || facetFieldList.size() == 0) {
                    logFileWriter.write("Could not write facet information for field " + field);
                    continue;
                }
                FacetField facetField = facetFieldList.get(0);
                String facetFieldName = facetField.getName();
                for(FacetField.Count count : facetFieldList.get(0).getValues()) {
                    localFacetFileWriter.append(facetFieldName).append(":").append(count.getName()).append(":")
                                        .append(Long.toString(count.getCount())).append(DEFAULT_NEWLINE);
                }
            }
            localFacetFileWriter.close();
            logFileWriter.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (SolrServerException e) {
            System.out.println(e.getMessage());
        }

        return !(!f.exists() || f.length() == 0) && addFile(hdfsFacetFilePath, f.getAbsolutePath());
    }


    public static void main(String[] args) throws Exception {
        String outputDir = args[1];

        setMRVariables(args[3]);
        suggCoreSolrServer = new HttpSolrServer(suggCoreSolrUrl);
        deleteIndex(suggCoreSolrServer);

        long currTime = System.currentTimeMillis();

        if (writeFacetFile()) {
            JobConf conf = new JobConf(SolrSuggestionCoreIndexer.class);
            conf.setJobName("SuggestionCoreIndexer");

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setMapperClass(Map.class);
            //conf.setReducerClass(Reduce.class);
            conf.set(JOB_MAP_KEY_SOLR_URL, suggCoreSolrUrl);
            conf.set(JOB_MAP_KEY_LOG_FILE, localLogFilePath);

            conf.setOutputFormat(TextOutputFormat.class);
            //FileInputFormat.setInputPaths(conf, new Path(inputDir));
            FileInputFormat.setInputPaths(conf, new Path(hdfsFacetFilePath));
            FileOutputFormat.setOutputPath(conf, new Path(outputDir));
            JobClient.runJob(conf);

            suggCoreSolrServer.commit();
        }

        System.out.println("Time elapsed: " + (System.currentTimeMillis() - currTime));

    }
}

