package service;

import GatesBigData.constants.HDFS;
import GatesBigData.utils.*;
import model.HDFSNutchCoreFileIterator;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.springframework.stereotype.Service;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static GatesBigData.constants.HDFS.*;
import static GatesBigData.utils.Utils.closeResource;

@Service
public class HDFSServiceImpl implements HDFSService {

    private static final Logger logger = Logger.getLogger(HDFSServiceImpl.class);

    public FileSystem getHDFSFileSystem() {
        try {
            return FileSystem.get(HDFS_URI, new Configuration(), USERNAME);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public Configuration getHDFSConfiguration() {
        return new Configuration();
    }

    public Configuration getNutchConfiguration() {
        return NutchConfiguration.create();
    }

    public int addAllFilesInLocalDirectory(String remoteFileDirectory, String localFileDirectory) {
        int nAdded = 0;

        try {
            FileSystem fs = getHDFSFileSystem();
            File localFileDir = new File(localFileDirectory);
            File[] localFiles = localFileDir.listFiles();

            if (localFiles != null && localFiles.length > 0) {
                for(File file : localFiles) {

                    Path srcPath = new Path(file.getAbsolutePath());
                    Path dstPath = new Path(remoteFileDirectory + "/" + file.getName());

                    if (!fs.exists(dstPath)) {
                        try {
                            fs.copyFromLocalFile(srcPath, dstPath);
                            nAdded++;
                        } catch(Exception e) {
                            logger.error(e.getMessage());
                        }
                    }
                }
            }

            fs.close();
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return nAdded;
    }

    public boolean addFile(String remoteFilePath, String localFilePath) {
        boolean success = false;

        try {
            FileSystem fs = getHDFSFileSystem();
            Path srcPath = new Path(localFilePath);
            Path dstPath = new Path(remoteFilePath);

            if (!fs.exists(dstPath)) {
                fs.copyFromLocalFile(srcPath, dstPath);
                success = fs.exists(dstPath);
            }

            closeResource(fs);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return success;
    }

    public boolean removeFile(String remoteFilePath) {
        FileSystem fs = getHDFSFileSystem();
        Path path = new Path(remoteFilePath);
        boolean success = false;

        try {
            if (fs.exists(path)) {
                success = fs.delete(path, true);
            }
            closeResource(fs);
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return success;
    }

    public String getFileContents(Path remoteFilePath) {
        return new String(getFileContentsAsBytes(remoteFilePath));
    }

    public byte[] getFileContentsAsBytes(Path remoteFilePath) {
        try {
            FileSystem fs = getHDFSFileSystem();
            if (!fs.exists(remoteFilePath)) {
                logger.error("File " + remoteFilePath.toString() + " does not exist on HDFS. ");
                return new byte[0];
            }

            long length = fs.getContentSummary(remoteFilePath).getLength();
            byte[] contents = new byte[(int)length];

            if (length > Integer.MAX_VALUE) {
                logger.error("File " + remoteFilePath.toString() + " is too large. ");
                return new byte[0];
            }

            DataInputStream d = new DataInputStream(fs.open(remoteFilePath));
            int offset = 0, numRead = 0;
            while (offset < contents.length && (numRead = d.read(contents, offset, contents.length-offset)) >= 0) {
                offset += numRead;
            }

            if (offset < contents.length) {
                throw new IOException("Could not completely read file " + remoteFilePath);
            }

            d.close();
            return contents;

        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return new byte[0];
    }

    public List<String> listFiles(Path path, boolean recurse, Pattern filter, FileSystem fs) {
        List<String> files = new ArrayList<String>();

        if (filter == null) {
            filter = HDFS.MATCH_ALL_PATTERN;
        }

        if (fs == null) {
            fs = getHDFSFileSystem();
        }
        try {
            for(FileStatus status : fs.listStatus(path)) {
                String child = status.getPath().toString();

                Matcher m = filter.matcher(child);
                if (m.matches()) {
                    files.add(HDFS.stripHDFSURI(child));
                }

                if (recurse && fs.isDirectory(status.getPath())) {
                    files.addAll(listFiles(status.getPath(), true, filter, fs));
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return files;
    }

    public List<String> listSegments(String coreName) {
        List<String> segments = new ArrayList<String>();

        for(String segPath : listFiles(HDFSUtils.getHDFSSegmentsDirectory(false, coreName), false, null, null)) {
            segments.add(new File(segPath).getName());
        }
        return segments;
    }

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer, boolean includeContentAndParseInfo) {
        long total = 0;
        HashMap<String, Long> filesPerSegment = new HashMap<String, Long>();
        HashMap<String, Long> fetched = new HashMap<String, Long>();

        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();
        Path segmentsDir = HDFSUtils.getHDFSSegmentsDirectory(false, coreName);

        HashMap<String, MapFile.Reader[]> parseTextReaders = includeContentAndParseInfo ? getSegmentToMapFileReaderMap(coreName, "getHDFSParseTextDir") : null;
        HashMap<String, MapFile.Reader[]> parseDataReaders = includeContentAndParseInfo ? getSegmentToMapFileReaderMap(coreName, "getHDFSParseDataDir") : null;

        try {
            for(String file : listFiles(segmentsDir, true, HDFSUtils.getCrawlFetchDataDirPattern(), fs)) {

                String seg = file.replaceAll(".*segments\\/(.*?)\\/.*$", "$1");
                writer.append("Segment ").append(seg).append(":\n");
                SequenceFile.Reader reader= new SequenceFile.Reader(fs, new Path(file), nutchConf);
                boolean moreTokens;

                do {
                    Text key = new Text();
                    CrawlDatum value = new CrawlDatum();
                    moreTokens = reader.next(key, value);

                    String statusName = CrawlDatum.getStatusName(value.getStatus());
                    String fileName = key.toString().equals("") ? "Empty" : key.toString();
                    String info = statusName + ":\t" + fileName;

                    if (includeContentAndParseInfo) {
                        info += (!Utils.nullOrEmpty(HDFSUtils.getParsedText(seg, key.toString(), parseTextReaders)) ? " PT " : "") +
                                (!Utils.nullOrEmpty(HDFSUtils.getParseData(seg, key.toString(), parseDataReaders)) ? " PD " : "");
                    }

                    writer.append("\t").append(info).append("\n");

                    long statusCt = fetched.containsKey(statusName) ? fetched.get(statusName) + 1 : 1;
                    fetched.put(statusName, statusCt);

                    filesPerSegment.put(seg, filesPerSegment.containsKey(seg) ? filesPerSegment.get(seg) + 1 : 1);
                    total++;
                } while (moreTokens);
                writer.append("\n");
            }
            writer.append("\nNumber of files\n ");
            writer.append("\tTotal:\t").append(Long.toString(total)).append("\n");
            for (Map.Entry<String, Long> entry : filesPerSegment.entrySet()) {
                writer.append("\t").append(entry.getKey()).append(":\t").append(Long.toString(entry.getValue())).append("\n");
            }

            writer.append("Fetched:\n");
            for(Map.Entry<String, Long> entry : fetched.entrySet()) {
                writer.append("\t").append(entry.getKey()).append(": ").append(Long.toString(entry.getValue())).append("\n");
            }

        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void writeFetched(String coreName, StringWriter writer) {
        writer.append("Fetched:\n");
        for(Map.Entry<String, Long> entry : getFetched(coreName).entrySet()) {
            writer.append("\t").append(entry.getKey()).append(": ").append(Long.toString(entry.getValue())).append("\n");
        }
    }

    public HashMap<String, Long> getFetched(String coreName)  {
        HashMap<String, Long> fetched = new HashMap<String, Long>();

        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();
        Path segmentsDir = HDFSUtils.getHDFSSegmentsDirectory(false, coreName);

        try {
            for(String file : listFiles(segmentsDir, true, HDFSUtils.getCrawlFetchDataDirPattern(), fs)) {

                SequenceFile.Reader reader= new SequenceFile.Reader(fs, new Path(file), nutchConf);
                boolean moreTokens;

                do {
                    Text key = new Text();
                    CrawlDatum value = new CrawlDatum();
                    moreTokens = reader.next(key, value);

                    String statusName = CrawlDatum.getStatusName(value.getStatus());
                    long statusCt = fetched.containsKey(statusName) ? fetched.get(statusName) + 1 : 1;
                    fetched.put(statusName, statusCt);

                } while (moreTokens);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        return fetched;
    }

    //Path dataFile = getHDFSContentDataFile(true, coreName, segment);
    public <T> T getContents(String coreName, Path dataFile, Class<T> clazz, Class<T> returnClass) throws IOException {
        HashMap<Text, Object> contents = new HashMap<Text, Object>();

        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, dataFile, nutchConf);
        do {
            try {
                Text key = new Text();
                T value = clazz.newInstance();

                if (!reader.next(key, (Writable) value)) break;
                contents.put(key, value);
            } catch (InstantiationException e) {
                logger.error(e.getMessage());
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage());
            }
        } while(true);


        return returnClass.cast(contents);
    }

    public HashMap<String, MapFile.Reader[]> getSegmentToMapFileReaderMap(String coreName, String methodName) {
        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        HashMap<String, MapFile.Reader[]> mapfileReaders = new HashMap<String, MapFile.Reader[]>();
        List<String> segments = listSegments(coreName);

        try {
            Method m = HDFSUtils.class.getDeclaredMethod(methodName, new Class[] { Boolean.class, String.class, String.class });
            m.setAccessible(true);

            for(String segment : segments) {
                Path dir = (Path) m.invoke(this, true, coreName, segment);
                if (fs.exists(dir)) {
                    mapfileReaders.put(segment, MapFileOutputFormat.getReaders(fs, dir, nutchConf));
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (NoSuchMethodException e) {
            logger.error(e.getMessage());
        } catch (InvocationTargetException e) {
            logger.error(e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
        }
        return mapfileReaders;
    }

    private HashMap<String, MapFile.Reader[]> getMapfileReader(Path dir, String segment) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        if (!fs.exists(dir)) {
            return null;
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(fs, dir, nutchConf));
        return map;
    }

    public Content getFileContents(String coreName, String segment, String fileName) {
        Path dir = HDFSUtils.getHDFSContentDir(true, coreName, segment);

        try {
            HashMap<String, MapFile.Reader[]> map = getMapfileReader(dir, segment);
            if (map != null) {
                return HDFSUtils.getFileContents(segment, fileName, map);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return HDFSUtils.fileDoesNotExistContent();
    }

    public ParseData getParseData(String coreName, String segment, String fileName) throws IOException {
        Path dir = HDFSUtils.getHDFSParseDataDir(true, coreName, segment);
        HashMap<String, MapFile.Reader[]> map = getMapfileReader(dir, segment);

        return (map == null) ? null : HDFSUtils.getParseData(segment, fileName, map);
        /*
        Path hdfsParseDataDir = HDFSUtils.getHDFSParseDataDir(true, coreName, segment);
        if (!hdfsFileSystem.exists(hdfsParseDataDir)) {
            return null;
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsParseDataDir, nutchConf));
        return HDFSUtils.getParseData(segment, fileName, map);   */
    }

    public String getParsedText(String coreName, String segment, String fileName) throws IOException {
        Path dir = HDFSUtils.getHDFSParseTextDir(true, coreName, segment);
        HashMap<String, MapFile.Reader[]> map = getMapfileReader(dir, segment);

        return (map == null) ? "" : HDFSUtils.getParsedText(segment, fileName, map);
    }

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        List<Content> contents = new ArrayList<Content>();

        Path hdfsContentDirectory = HDFSUtils.getHDFSContentDir(true, coreName, segment);
        if (fs.exists(hdfsContentDirectory)) {
            MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, hdfsContentDirectory, getNutchConfiguration());
            contents = HDFSUtils.getContentList(fileNames, readers);
        }
        return contents;
    }

    public void testCrawlData(String coreName, String segment) {
        //try {
            FileSystem fs = getHDFSFileSystem();
            Configuration conf = NutchConfiguration.create();

            List<String> segments = listSegments(coreName);

            HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(segments, conf, fs, null);
            iter.getNextNFileNames(100);

            for(String s : listSegments(coreName)) {
                //TreeMap<Text, CrawlDatum> allContents = getCrawlData(HDFSUtils.getHDFSCrawlParseDataFile(true, coreName, s));
            }
            //crawlDbReader.processStatJob(getHDFSCrawlDBCurrentIndexFile(true, coreName).toString(), conf, true);
            //TreeMap<Text, CrawlDatum> allContents = getCrawlData(getHDFSCrawlDBCurrentDataFile(true, coreName));
           // getContents(getHDFSContentDataFile(true, coreName, segment), Content.class, HashMap<Text, Content>.class);
        //} catch (IOException e) {
        //    System.out.println(e.getMessage());
        //}
    }

    public Content getContent(String coreName, String segment, String filePath, StringWriter writer) throws IOException {
        Content content = getFileContents(coreName, segment, filePath);
        if (content == null) {
            Utils.printFileErrorMessage(writer, "Null content");
            return null;
        }
        if (HDFSUtils.contentIndicatesFileDoesNotExist(content)) {
            Utils.printFileErrorMessage(writer, "File " + filePath + " does not exist on HDFS");
            return null;
        }
        return content;
    }
}
