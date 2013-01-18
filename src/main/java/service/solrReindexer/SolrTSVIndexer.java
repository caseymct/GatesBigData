import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrTSVIndexer {

    public static SolrServer SERVER;
    public static String FIELDS                 = "";
    public static String DELIMITER              = ",";
    public static final String SOLR_URL_BASE    = "http://denlx006.dn.gates.com:8984/solr/";
    public static String SOLR_URL               = "";

    public static class Map extends Configured implements Mapper<LongWritable, Text, Text, Text> {
        public static SolrServer SERVER;
        public static String[] FIELDS;
        public static String DELIMITER;
        public static String SOLR_URL;

        public void configure(JobConf job) {
            setConf(job);
            SOLR_URL = job.get("SOLR_URL");
            SERVER = new HttpSolrServer(SOLR_URL);
            DELIMITER = job.get("DELIMITER");
            FIELDS = job.get("FIELDS").split(DELIMITER);
        }

        public void close() throws IOException { }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            SolrInputDocument solrDoc = new SolrInputDocument();
            String fieldInfo = value.toString().replaceAll("^\"|\"$", "");
            if (fieldInfo.trim().equals("")) return;

            String[] fieldValues = fieldInfo.split(DELIMITER);
            solrDoc.addField("id", UUID.randomUUID());

            if (!fieldValues[0].contains(FIELDS[0])) {
                for(int i = 0; i < fieldValues.length; i++) {
                    String fieldName = (i < FIELDS.length) ? FIELDS[i] : "FIELD_" + i;
                    solrDoc.addField(fieldName, fieldValues[i]);
                }

                try {
                    SERVER.add(solrDoc);

                } catch (SolrServerException e) {
                    output.collect(new Text(e.getMessage()), new Text(""));
                    System.err.println(e.getMessage());
                }

                output.collect(new Text(fieldValues.length + ""), new Text(fieldInfo));
            }
        }
    }

    public static class Reduce extends Configured implements Reducer<Text, Text, Text, Text> {
        public void configure(JobConf job) {
            setConf(job);
        }

        public void close() throws IOException { }

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String indexedToSolr = "-->";
            while (values.hasNext()) {
                indexedToSolr += values.next().toString() + ", ";
            }
            output.collect(key, new Text(indexedToSolr));
        }
    }

    public static void setMRVariables(String fileName) {
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = "";

            while ((line = br.readLine()) != null) {
                String[] lineComponents = line.split("=");
                String key = lineComponents[0].toUpperCase();
                String val = lineComponents[1];
                if (key.equals("CORENAME")) {
                    SOLR_URL = SOLR_URL_BASE + val;
                } else if (key.equals("DELIMITER")) {
                    DELIMITER = val;
                } else if (key.equals("FIELDS")) {
                    FIELDS = val;
                }
            }
            in.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
    public static void main(String[] args) throws Exception {
        String coreName = args[0];
        String outputDir = args[1];
        String inputDir = args[2];

        setMRVariables(args[3]);

        /*
        String solrHome = "/projects/solr/solr4.0/multicoreexample/";
        File solrXml = new File(solrHome + "solr/solr.xml");
        CoreContainer coreContainer = new CoreContainer(solrHome, solrXml);
        EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, "NA_data");
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("*:*");
        QueryResponse rsp = server.query(solrQuery);
        System.out.println(rsp.getResults().getNumFound());  */

        // Delete the index.
        SERVER = new HttpSolrServer(SOLR_URL);
        SERVER.deleteByQuery("*:*");
        SERVER.commit();

        long currTime = System.currentTimeMillis();

        JobConf conf = new JobConf(SolrTSVIndexer.class);
        conf.setJobName("TSVtest");
        //conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        //conf.setInputFormat(TSVRecordInputFormat.class);

        //conf.set("textinputformat.record.delimiter", "\"");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);

        conf.set("FIELDS", FIELDS);
        conf.set("DELIMITER", DELIMITER);
        conf.set("SOLR_URL", SOLR_URL);
        //conf.setReducerClass(Reduce.class);

        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(inputDir));
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));
        JobClient.runJob(conf);

        SERVER.commit();

        System.out.println("Time elapsed: " + (System.currentTimeMillis() - currTime));
    }
}