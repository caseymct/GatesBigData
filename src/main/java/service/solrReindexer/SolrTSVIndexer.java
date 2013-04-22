import service.solrReindexer.ReindexerUtils;
import org.apache.hadoop.conf.Configured;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrTSVIndexer {

    public static HashMap<String, String> argMap;
    public static File dataInputFile;
    public static String hdfsInputFile;
    public static SolrServer solrServer;
    public static String coreName;
    public static boolean writeInputFile;
    public static boolean deleteIndex;
    public static boolean debug;
    public static int printInterval = 10000;

    public static class Map extends Configured implements Mapper<LongWritable, Text, Text, Text> {
        public static SolrServer mapSolrServer;
        public static List<String> mapFields;
        public static String delimiter;

        public void configure(JobConf job) {
            setConf(job);

            delimiter       = job.get(ReindexerUtils.JOB_MAP_KEY_DELIMITER);
            mapFields       = ReindexerUtils.getTokens(job.get(ReindexerUtils.JOB_MAP_KEY_FIELDS), delimiter);
            mapSolrServer   = new HttpSolrServer(job.get(ReindexerUtils.JOB_MAP_KEY_SOLRURL));
        }

        public void close() throws IOException {}

        public String addRecordIfNonExistant(String id, SolrInputDocument doc) {
            SolrQuery query = new SolrQuery(ReindexerUtils.SOLR_KEY_ID + ":" + id);
            query.setRows(0);

            try {
                QueryResponse rsp = mapSolrServer.query(query);
                if (rsp == null || rsp.getResults().getNumFound() == 0) {
                    mapSolrServer.add(doc);
                    return "Adding id " + id;
                } else {
                    return "Did not add " + id + ", nFound " + rsp.getResults().getNumFound();
                }
            } catch (SolrServerException e) {
                return "id " + id + " threw error " + e.getMessage();
            } catch (IOException e) {
                return "id " + id + " threw error " + e.getMessage();
            }
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String fieldInfo = value.toString().replaceAll("^\"|\"$", "");
            List<String> fieldValues = ReindexerUtils.getTokens(fieldInfo, delimiter);

            if (fieldInfo.trim().equals("") || fieldValues.get(0).equals(mapFields.get(0))) {
                return;
            }

            String result;
            if (mapFields.size() != fieldValues.size()) {
                result = "Ignoring id " + fieldValues.get(0) + " because mapfields.size is " + mapFields.size() +
                         " and fieldValues.size is " + fieldValues.size();
            } else {
                SolrInputDocument solrDoc = new SolrInputDocument();
                for(String field : mapFields) {
                    String fieldValue = fieldValues.get(mapFields.indexOf(field));
                    if (fieldValue.equals("null")) fieldValue = null;
                    solrDoc.addField(field, fieldValue);
                }
                result = addRecordIfNonExistant(fieldValues.get(0), solrDoc);
            }

            output.collect(new Text(result), new Text(""));
        }
    }

    public static class Reduce extends Configured implements Reducer<Text, Text, Text, Text> {
        public void configure(JobConf job) {
            setConf(job);
        }

        public void close() throws IOException { }

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String indexedToSolr = "INDEXED: ";
            while (values.hasNext()) indexedToSolr += values.next().toString() + ", ";
            output.collect(key, new Text(indexedToSolr + "\n"));
        }
    }

    public static void setMRVariables(String fileName) {
        argMap = ReindexerUtils.setMRVariables(fileName);

        coreName          = argMap.get(ReindexerUtils.CONF_KEY_CORENAME);
        hdfsInputFile     = argMap.get(ReindexerUtils.CONF_KEY_HDFS_INPUTFILE);
        dataInputFile     = new File(argMap.get(ReindexerUtils.CONF_KEY_INPUTFILE_PATH));
        writeInputFile    = ReindexerUtils.returnBoolValueIfExists(argMap, ReindexerUtils.CONF_KEY_WRITE_INPUTFILE);
        deleteIndex       = ReindexerUtils.returnBoolValueIfExists(argMap, ReindexerUtils.CONF_KEY_DELETE_INDEX);
        debug             = ReindexerUtils.returnBoolValueIfExists(argMap, ReindexerUtils.CONF_KEY_DEBUG);
        solrServer        = new HttpSolrServer(argMap.get(ReindexerUtils.CONF_KEY_SOLR_URL));
    }

    public static boolean writeInputFile() {
        String delimiter = argMap.get(ReindexerUtils.CONF_KEY_DELIMITER);
        boolean exists  = dataInputFile.exists();
        String fullPath = dataInputFile.getAbsolutePath();

        try {
            ReindexerUtils.debugPrint(debug, false, "Data input file " + fullPath + " exists: " + exists);
            if (exists) {
                boolean deleteSuccess = dataInputFile.delete();
                ReindexerUtils.debugPrint(debug, false, "Data input file " + fullPath + " delete success: " + deleteSuccess);
            }

            FileOutputStream out = new FileOutputStream(fullPath);

            ResultSet result = ReindexerUtils.getJDBCResultSet(coreName);
            ResultSetMetaData metaData = result.getMetaData();
            int nCols = metaData.getColumnCount();
            List<String> names = new ArrayList<String>();
            List<String> types = new ArrayList<String>();

            names.add(ReindexerUtils.SOLR_KEY_ID);
            types.add(ReindexerUtils.SQL_TYPE_STRING);
            for(int n = 1; n <= nCols; n++) {
                names.add(metaData.getColumnName(n));
                types.add(metaData.getColumnTypeName(n));
            }

            ReindexerUtils.writeToFile(ReindexerUtils.join(names, delimiter), out);

            long i = 0;
            while (result.next()) {
                List<String> data = new ArrayList<String>();
                for(int n = 1; n <= nCols; n++) {
                    data.add(ReindexerUtils.formatColumnObject(result.getObject(n), types.get(n)));
                }
                data.add(0, ReindexerUtils.constructUUIDStringFromRowEntry(names.subList(1, names.size()), data));

                ReindexerUtils.writeToFile(ReindexerUtils.join(data, delimiter), out);
                ReindexerUtils.debugPrint(debug && (i % printInterval == 0), false, "# Records written so far: " + i);
                i++;
            }

            out.close();
            out.flush();

            return dataInputFile.exists() && dataInputFile.length() > 0 && ReindexerUtils.addFileToHDFS(hdfsInputFile, fullPath);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return false;
    }


    public static void main(String[] args) throws Exception {
        String outputDir = args[1];
        String inputDir  = args[2];
        setMRVariables(args[3]);

        long currTime = System.currentTimeMillis();

        if (deleteIndex) {
            ReindexerUtils.debugPrint(debug, false, "Deleting index for core " + coreName);
            ReindexerUtils.deleteIndex(solrServer);
        }

        if (!writeInputFile || writeInputFile()) {
            ReindexerUtils.debugPrint(debug, false, "Running re-index on core " + coreName);

            JobConf conf = new JobConf(SolrTSVIndexer.class);
            conf.setJobName("StructuredIndexer");
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setMapperClass(Map.class);

            conf.set(ReindexerUtils.JOB_MAP_KEY_FIELDS,    argMap.get(ReindexerUtils.CONF_KEY_FIELDS));
            conf.set(ReindexerUtils.JOB_MAP_KEY_DELIMITER, argMap.get(ReindexerUtils.CONF_KEY_DELIMITER));
            conf.set(ReindexerUtils.JOB_MAP_KEY_SOLRURL,   argMap.get(ReindexerUtils.CONF_KEY_SOLR_URL));
            conf.set(ReindexerUtils.JOB_MAP_KEY_LOGFILE,   argMap.get(ReindexerUtils.CONF_KEY_LOGFILE));

            conf.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(conf, new Path(inputDir));
            FileOutputFormat.setOutputPath(conf, new Path(outputDir));
            JobClient.runJob(conf);

            boolean updateSuccess       = ReindexerUtils.solrServerUpdate(solrServer, null);
            boolean addInfoFilesSuccess = ReindexerUtils.addInfoFiles(coreName, solrServer);

            ReindexerUtils.debugPrint(debug, false, "Time elapsed      : " + (System.currentTimeMillis() - currTime));
            ReindexerUtils.debugPrint(debug, false, "Update success    : " + updateSuccess);
            ReindexerUtils.debugPrint(debug, false, "Added info files? : " + addInfoFilesSuccess);
        }
    }
}
