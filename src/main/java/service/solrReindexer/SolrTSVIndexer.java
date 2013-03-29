import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrTSVIndexer {

    public static final String DEFAULT_DELIMITER = ",";
    public static final String SOLR_URL_BASE     = "http://denlx006.dn.gates.com:8984/solr/";
    public static final String SOLR_KEY_ID       = "id";
    public static final String LOG_FILE          = "/tmp/tsvlog";

    public static final String CONF_KEY_CORENAME      = "CORENAME";
    public static final String CONF_KEY_FIELDS        = "FIELDS";
    public static final String CONF_KEY_DELIMITER     = "DELIMITER";
    public static final String CONF_KEY_LOGFILE       = "LOGFILE";

    public static final String JOB_MAP_KEY_FIELDS        = "FIELDS";
    public static final String JOB_MAP_KEY_SOLRURL       = "SOLR_URL";
    public static final String JOB_MAP_KEY_DELIMITER     = "DELIMITER";
    public static final String JOB_MAP_KEY_LOGFILE       = "LOGFILE";

    public static String logFileName;
    public static SolrServer solrServer;
    public static String coreName;
    public static String solrUrl;
    public static String fields;
    public static String fieldsSubset;
    public static String delimiter = DEFAULT_DELIMITER;

    public static class Map extends Configured implements Mapper<LongWritable, Text, Text, Text> {
        public static SolrServer mapSolrServer;
        public static List<String> mapFields;
        public static String delimiter;
        public static BufferedWriter mapLogWriter;
        public static SimpleDateFormat sdf  = new SimpleDateFormat("dd-MMM-yy");
        public static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-mm-dd");

        public static SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
        public static Pattern shortDatePattern  = Pattern.compile("^(\\d{2})-(\\w{3})-(\\d{2})$");
        public static Pattern shortDatePattern2 = Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})$");

        public static HashMap<Pattern, SimpleDateFormat> dateFormatHashMap = new HashMap<Pattern, SimpleDateFormat>() {{
            put(Pattern.compile("^(\\d{2})-(\\w{3})-(\\d{2})$"), new SimpleDateFormat("dd-MMM-yy"));
            put(Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})$"), new SimpleDateFormat("yyyy-mm-dd"));
        }};

        public void configure(JobConf job) {
            setConf(job);

            delimiter       = job.get(JOB_MAP_KEY_DELIMITER);
            mapFields       = getTokens(job.get(JOB_MAP_KEY_FIELDS));
            mapSolrServer   = new HttpSolrServer(job.get(JOB_MAP_KEY_SOLRURL));

            /*File logFile = new File(job.get(JOB_MAP_KEY_LOGFILE));
            try {
                mapLogWriter = new BufferedWriter(new FileWriter(logFile, true));
            } catch (IOException e) {
                System.out.println(e.getMessage());
            } */
        }

        public void close() throws IOException {
            //mapLogWriter.close();
        }

        public List<String> getTokens(String d) {
            List<String> tokens = new ArrayList<String>();
            if (d.equals("")) return tokens;

            StringTokenizer tk = new StringTokenizer(d, delimiter, true);
            String token = "", prevToken = "";
            int nTokens = tk.countTokens();
            for(int i = 0; i < nTokens; i++) {
                prevToken = token;
                token = (String) tk.nextElement();
                if (!token.equals(delimiter)) {
                    tokens.add(token);
                } else {
                    if (prevToken.equals(delimiter)) {
                        tokens.add("");
                    }
                    if (i == nTokens - 1) {
                        tokens.add("");
                    }
                }
            }
            return tokens;
        }

        /*public static void writeToLog(String s) {
            try {
                mapLogWriter.write(s + "\n");
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }  */

        public String checkIfMatches(Pattern p, SimpleDateFormat simpleDateFormat, String s) {
            Matcher m = p.matcher(s);
            if (m.matches()) {
                try {
                    return solrDateFormat.format(simpleDateFormat.parse(s));
                } catch (ParseException e) {}
            }
            return null;
        }

        public Object formatIfDate(Object val) {
            String newDate = null;
            for(java.util.Map.Entry<Pattern, SimpleDateFormat> entry : dateFormatHashMap.entrySet()) {
                newDate = checkIfMatches(entry.getKey(), entry.getValue(), val.toString());
                if (newDate != null) {
                    return newDate;
                }
            }
            return val;
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String fieldInfo = value.toString().replaceAll("^\"|\"$", "");
            List<String> fieldValues = getTokens(fieldInfo);

            if (fieldInfo.trim().equals("") || fieldValues.get(0).equals(mapFields.get(0))) {
                return;
            }

            if (mapFields.size() != fieldValues.size()) {
                //writeToLog("Bad size! fields: " + mapFields.size() + ", fieldValues: " + fieldValues.size());
                //writeToLog(StringUtils.join(fieldValues, "~") + "\n");
                return;
            }

            SolrInputDocument solrDoc = new SolrInputDocument();
            solrDoc.addField(SOLR_KEY_ID, UUID.randomUUID());

            for(String field : mapFields) {
                Object val = fieldValues.get(mapFields.indexOf(field));
                /*Matcher m = shortDatePattern.matcher(val.toString());
                if (m.matches()) {
                    try {
                        val = solrDateFormat.format(sdf.parse(val.toString()));
                    } catch (ParseException e) {
                        val = null;
                        System.err.println(e.getMessage());
                    }
                }    */
                val = formatIfDate(val);
                if (val != null && val.toString().equals("")) {
                    val = null;
                }
                solrDoc.addField(field, val);
            }

            try {
                mapSolrServer.add(solrDoc);
            } catch (SolrServerException e) {
                output.collect(new Text(e.getMessage()), new Text(""));
                //writeToLog("Solr Server exception: " + e.getMessage());
            }

            output.collect(new Text(fieldValues.size() + ""), new Text(fieldInfo));
        }
    }

    public static class Reduce extends Configured implements Reducer<Text, Text, Text, Text> {
        public void configure(JobConf job) {
            setConf(job);
        }

        public void close() throws IOException { }

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String indexedToSolr = "INDEXED: ";
            while (values.hasNext()) {
                indexedToSolr += values.next().toString() + ", ";
            }
            indexedToSolr += "\n";
            output.collect(key, new Text(indexedToSolr));
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
        logFileName = LOG_FILE;
        fieldsSubset = "";

        try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            BufferedReader br  = new BufferedReader(new InputStreamReader(in));
            String line;

            while ((line = br.readLine()) != null) {
                String[] lineComponents = line.split("=");
                String key = lineComponents[0].toUpperCase();
                String val = lineComponents[1];
                if (key.equals(CONF_KEY_CORENAME)) {
                    coreName = val;
                    solrUrl = SOLR_URL_BASE + coreName;
                } else if (key.equals(CONF_KEY_DELIMITER)) {
                    delimiter = val;
                } else if (key.equals(CONF_KEY_FIELDS)) {
                    fields = val;
                } else if (key.equals(CONF_KEY_LOGFILE)) {
                    logFileName = val;
                }
            }
            in.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        String outputDir = args[1];
        String inputDir = args[2];

        setMRVariables(args[3]);

        long currTime = System.currentTimeMillis();

        solrServer = new HttpSolrServer(solrUrl);
        deleteIndex(solrServer);

        JobConf conf = new JobConf(SolrTSVIndexer.class);
        conf.setJobName("StructuredIndexer");
        //conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        //conf.setReducerClass(Reduce.class);

        conf.set(JOB_MAP_KEY_FIELDS, fields);
        conf.set(JOB_MAP_KEY_DELIMITER, delimiter);
        conf.set(JOB_MAP_KEY_SOLRURL, solrUrl);
        conf.set(JOB_MAP_KEY_LOGFILE, logFileName);

        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(inputDir));
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));
        JobClient.runJob(conf);

        solrServer.commit();

        System.out.println("Time elapsed: " + (System.currentTimeMillis() - currTime));

    }
}
