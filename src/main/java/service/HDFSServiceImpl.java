package service;

import GatesBigData.utils.*;
import model.HDFSNutchCoreFileIterator;
import model.NotifyingThread;
import model.ThreadCompleteListener;
import net.sf.json.JSONObject;
import org.apache.commons.net.util.Base64;
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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;

@Service
public class HDFSServiceImpl implements HDFSService, ThreadCompleteListener {

    private DocumentConversionService documentConversionService;


    private static final String THUMBNAILS_DIR  = "thumbnails";
    private static final String HDFS_USERNAME   = "hdfs";

    private static final int N_THUMBNAIL_THREADS = 10;
    private List<Thread> thumbnailThreads = new ArrayList<Thread>();

    private static final Logger logger = Logger.getLogger(HDFSServiceImpl.class);

    @Autowired
    public void setServices(DocumentConversionService documentConversionService) {
        this.documentConversionService = documentConversionService;
    }

    public FileSystem getHDFSFileSystem() {
        try {
            return FileSystem.get(URI.create(HDFSUtils.getHdfsUri()), new Configuration(), HDFS_USERNAME);
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
                            System.err.println(e.getMessage());
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
        try {
            FileSystem fs = getHDFSFileSystem();
            Path srcPath = new Path(localFilePath);
            Path dstPath = new Path(remoteFilePath);

            if (fs.exists(dstPath)) {
                return false;
            }

            try {
                fs.copyFromLocalFile(srcPath, dstPath);
            } catch(Exception e) {
                System.err.println(e.getMessage());
                return false;
            } finally {
                fs.close();
            }
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean removeFile(String remoteFilePath) {
        try {
            FileSystem fs = getHDFSFileSystem();
            Path path = new Path(remoteFilePath);

            if (fs.exists(path)) {
                fs.delete(path, true);
                return true;
            }

            fs.close();
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return false;
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
            while (offset < contents.length
                    && (numRead = d.read(contents, offset, contents.length-offset)) >= 0) {
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

    private String returnResponseIfNoError(String response) {
        if (!HDFSUtils.hasHDFSErrorString(response)) {
            return response;
        }
        return null;
    }

    public String getInfoFileContents(String coreName, String type) {
        String contents = "";

        if (type.equals(Constants.SOLR_FACET_FIELDS_ID_FIELD_NAME)) {
            contents = getFileContents(HDFSUtils.getHDFSFacetFieldsCustomFile(coreName));
        } else if (type.equals(Constants.SOLR_VIEW_FIELDS_ID_FIELD_NAME)) {
            contents = getFileContents(HDFSUtils.getHDFSViewFieldsCustomFile(coreName));
        } else if (type.equals(Constants.SOLR_PREVIEW_FIELDS_ID_FIELD_NAME)) {
            contents = getFileContents(HDFSUtils.getHDFSPreviewFieldsCustomFile(coreName));
        }

        return returnResponseIfNoError(contents);
    }

    public List<String> getHDFSPreviewFields(String coreName) {
        String response = getInfoFileContents(coreName, Constants.SOLR_PREVIEW_FIELDS_ID_FIELD_NAME);
        return Utils.nullOrEmpty(response) ? null : Arrays.asList(response.split(","));
    }

    public HashMap<String, String> getInfoFilesContents(String coreName) {
        HashMap<String, String> infoFileContents = new HashMap<String, String>();

        for(String infoField : Constants.SOLR_INFO_FILES_LIST) {
            String content = getInfoFileContents(coreName, infoField);
            if (!Utils.nullOrEmpty(content)) {
                infoFileContents.put(infoField, content);
            }
        }
        return infoFileContents;
    }

    public List<String> listFiles(Path hdfsDirectory, boolean recurse) {
        List<String> filePaths = new ArrayList<String>();

        FileSystem fs = getHDFSFileSystem();
        int hdfsUriStringLength = HDFSUtils.getHdfsUri().length();

        try {
            if (recurse) {
                RemoteIterator<LocatedFileStatus> r = fs.listFiles(hdfsDirectory, true);
                while(r.hasNext()) {
                    String pathString = r.next().getPath().toString();
                    // just add the HDFS path: hdfs://denlx006.dn.gates.com/path/etc --> /path/etc
                    filePaths.add(pathString.substring(hdfsUriStringLength));
                }
            } else {
                for(FileStatus status : fs.listStatus(hdfsDirectory)) {
                    filePaths.add(status.getPath().getName());
                }
            }
        } catch (FileNotFoundException e){
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return filePaths;
    }

    public List<String> listSegments(String coreName) {
        return listFiles(HDFSUtils.getHDFSSegmentsDirectory(false, coreName), false);
    }

    public void listFilesInCrawlDirectory(String coreName, StringWriter writer) {
        long total = 0;

        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(writer);

            g.writeStartObject();

            for(String segment : listSegments(coreName)) {
                g.writeArrayFieldStart(segment);

                for(Map.Entry entry : getCrawlData(HDFSUtils.getHDFSCrawlFetchDataFile(true, coreName, segment)).entrySet()) {
                    g.writeString(entry.getKey().toString() + ", " + ((CrawlDatum) entry.getValue()).getStatus());
                    total++;
                }
                g.writeEndArray();
            }

            g.writeNumberField("Number of files: ", total);
            g.writeEndObject();
            g.close();

        } catch (IOException e) {
            System.out.println(e);
        }
    }

    private TreeMap<Text, CrawlDatum> getCrawlData(Path path) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        TreeMap<Text, CrawlDatum> allContents = new TreeMap<Text, CrawlDatum>();
        SequenceFile.Reader reader= new SequenceFile.Reader(fs, path, nutchConf);
        do {
            Text key = new Text();
            CrawlDatum value = new CrawlDatum();
            if (!reader.next(key, value)) break;
            allContents.put(key, value);
        } while (true);

        return allContents;
    }

    public long getFetched(String coreName)  {
        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();
        Path path = HDFSUtils.getHDFSCrawlDBCurrentIndexFile(true, coreName);
        long fetched = 0L;

        try {
            SequenceFile.Reader reader= new SequenceFile.Reader(fs, path, nutchConf);
            do {
                Text key = new Text();
                CrawlDatum value = new CrawlDatum();
                if (!reader.next(key, value)) break;
                if (value.getStatus() == 2) {
                    fetched++;
                }
            } while (true);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return fetched;
    }

    public <T> T getContents(String coreName, Path dataFile, Class<T> clazz, Class<T> returnClass) throws IOException {
        HashMap<Text, Object> contents = new HashMap<Text, Object>();

        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        //Path dataFile = getHDFSContentDataFile(true, coreName, segment);
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



    public Path getHDFSThumbnailPathFromHDFSDocPath(String coreName, String segment, String hdfsPath) {
        String imgName = documentConversionService.getThumbnailNameFromHDFSPath(hdfsPath);
        Path thumbnailPath = new Path(HDFSUtils.getHDFSCoreDirectory(false, coreName), THUMBNAILS_DIR);
        Path segmentPath = new Path(thumbnailPath, segment);
        return new Path(segmentPath, imgName);
    }

    public SequenceFile.Reader getSequenceFileReader(Path dir) {
        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        try {
            if (fs.exists(dir)) {
                return new SequenceFile.Reader(fs, dir, nutchConf);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return null;
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

    public void notifyOfThreadComplete(final Thread thread) {
        int index = thumbnailThreads.indexOf(thread);
        if (index >= 0) {
            thumbnailThreads.remove(index);
        }
        if (thumbnailThreads.size() == 0) {
            System.out.println("Done creating thumbnails.");
        }
    }

    public void generateThumbnails(String coreName) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        Configuration nutchConf = getNutchConfiguration();

        List<String> segments = listSegments(coreName);

        HashMap<String, MapFile.Reader[]> mapfileReaders = getSegmentToMapFileReaderMap(coreName, "getHDFSContentDirectory");

        Path hdfsCoreDir  = HDFSUtils.getHDFSCoreDirectory(false, coreName);
        Path thumbnailDir = returnOrCreateDirectory(new Path(hdfsCoreDir, THUMBNAILS_DIR));
        HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(segments, nutchConf, fs,
                HDFSUtils.getHDFSCrawlFetchDataFile(true, coreName, "00000"));

        for(int i = 0; i < N_THUMBNAIL_THREADS; i++) {
            GenerateThumbnailThread worker = new GenerateThumbnailThread(iter, thumbnailDir, coreName, fs, mapfileReaders);
            worker.addListener(this);
            worker.setName("ThreadSeg_" + i);
            worker.start();
            thumbnailThreads.add(worker);
        }
    }

    private Path returnOrCreateDirectory(Path path) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        return path;
    }

    class GenerateThumbnailThread extends NotifyingThread {
        HDFSNutchCoreFileIterator fileIterator;
        Path thumbnailDir;
        String coreName;
        FileSystem fs;
        HashMap<String, MapFile.Reader[]> mapfileReaders;
        HashMap<String, String> files;
        int N_FILES = 100;

        GenerateThumbnailThread(HDFSNutchCoreFileIterator fileIterator, Path thumbnailDir, String coreName, FileSystem fs,
                                HashMap<String, MapFile.Reader[]> mapfileReaders) {
            this.fileIterator = fileIterator;
            this.thumbnailDir = thumbnailDir;
            this.coreName = coreName;
            this.fs = fs;
            this.mapfileReaders = mapfileReaders;
        }

        private void createThumbnails() {
            try {
                for(Map.Entry<String, String> entry : this.files.entrySet()) {
                    String fileName = entry.getKey();
                    String segment = entry.getValue();
                    returnOrCreateDirectory(new Path(thumbnailDir, segment));

                    Content content = HDFSUtils.getFileContents(segment, fileName, mapfileReaders);
                    String thumbnail = documentConversionService.convertContentToThumbnail(content, fileName);

                    if (!thumbnail.equals("") && !Utils.hasFileErrorMessage(thumbnail)) {
                        Path hdfsThumbnailPath = getHDFSThumbnailPathFromHDFSDocPath(coreName, segment, fileName);
                        if (fs.exists(hdfsThumbnailPath)) {
                            fs.delete(hdfsThumbnailPath, false);
                        }
                        fs.moveFromLocalFile(new Path(thumbnail), hdfsThumbnailPath);
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        public void doRun() {
            while (!fileIterator.done()) {
                this.files = fileIterator.getNextNFileNames(N_FILES);
                createThumbnails();
            }
        }
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
        Path dir = HDFSUtils.getHDFSContentDirectory(true, coreName, segment);

        try {
            HashMap<String, MapFile.Reader[]> map = getMapfileReader(dir, segment);
            if (map != null) {
                return HDFSUtils.getFileContents(segment, fileName, map);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return HDFSUtils.fileDoesNotExistContent();
        /*if (!hdfsFileSystem.exists(hdfsContentDirectory)) {
            return HDFSUtils.fileDoesNotExistContent();
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsContentDirectory, nutchConf));

        return HDFSUtils.getFileContents(segment, fileName, map); */
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
        /*
        Path hdfsParseTextDir = HDFSUtils.getHDFSParseTextDir(true, coreName, segment);
        if (!hdfsFileSystem.exists(hdfsParseTextDir)) {
            return "";
        }

        HashMap<String, MapFile.Reader[]> map = new HashMap<String, MapFile.Reader[]>();
        map.put(segment, MapFileOutputFormat.getReaders(hdfsFileSystem, hdfsParseTextDir, nutchConf));

        return HDFSUtils.getParsedText(segment, fileName, map); */
    }

    public List<Content> getFileContents(String coreName, String segment, List<String> fileNames) throws IOException {
        FileSystem fs = getHDFSFileSystem();
        List<Content> contents = new ArrayList<Content>();

        Path hdfsContentDirectory = HDFSUtils.getHDFSContentDirectory(true, coreName, segment);
        if (fs.exists(hdfsContentDirectory)) {
            MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, hdfsContentDirectory, getNutchConfiguration());
            contents = HDFSUtils.getContentList(fileNames, readers);
            /*for(String fileName : fileNames) {
                Text key = new Text(fileName);
                Content content = new Content();
                MapFileOutputFormat.getEntry(readers, PARTITIONER, key, content);
                contents.add(content);
            }   */
        }
        return contents;
    }

    public void testCrawlData(String coreName, String segment) {
        try {
            FileSystem fs = getHDFSFileSystem();
            Configuration conf = NutchConfiguration.create();

            List<String> segments = listSegments(coreName);

            HDFSNutchCoreFileIterator iter = new HDFSNutchCoreFileIterator(segments, conf, fs, HDFSUtils.getHDFSCrawlFetchDataFile(true, coreName, "00000"));
            iter.getNextNFileNames(100);

            for(String s : listSegments(coreName)) {
                TreeMap<Text, CrawlDatum> allContents = getCrawlData(HDFSUtils.getHDFSCrawlParseDataFile(true, coreName, s));
            }
            //crawlDbReader.processStatJob(getHDFSCrawlDBCurrentIndexFile(true, coreName).toString(), conf, true);
            //TreeMap<Text, CrawlDatum> allContents = getCrawlData(getHDFSCrawlDBCurrentDataFile(true, coreName));
           // getContents(getHDFSContentDataFile(true, coreName, segment), Content.class, HashMap<Text, Content>.class);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
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
