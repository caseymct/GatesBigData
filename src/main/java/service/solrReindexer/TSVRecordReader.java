package service.solrReindexer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class TSVRecordReader extends RecordReader<LongWritable, Text> {

    // reference to the logger
    private static final Log LOG = LogFactory.getLog(TSVRecordReader.class);

    private final int NLINESTOPROCESS = 3;
    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long splitStart = 0;
    private long splitEnd = 0;
    private long pos = 0;
    private int maxLineLength = Integer.MAX_VALUE;

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    private long currentPosition;               // our current position in the split
    private int nfieldsRead = 0;
    //private int recordLength;                   // the length of a record
    private int nfields;                        // number of fields to parse
    private FSDataInputStream fileInputStream;  // reference to the input stream
    private Counter inputByteCounter;           // the input byte counter
    private FileSplit fileSplit;                // reference to our FileSplit


    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (splitStart == splitEnd) ? 0.0f : Math.min(1.0f, (pos - splitStart) / (float)(splitEnd - splitStart));
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
        this.nfields = 74;

        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        //this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        splitStart = split.getStart();
        splitEnd = splitStart + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());

        if (splitStart != 0){
            //skipFirstLine = true;
            //--splitStart;
            filein.seek(splitStart);
        }
        in = new LineReader(filein,conf);
        if(skipFirstLine){
            splitStart += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, splitEnd - splitStart));
        }
        this.pos = splitStart;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        char delim = '~';
        byte delimByte = (byte) delim;
        int totalRead = 0;

        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n");
        int read = 0;
        nfieldsRead = 0;


        if (pos < splitEnd) {
            while (nfieldsRead < nfields) {
                //Text v = new Text();
                byte[] buffer = new byte[1];
                read = this.fileInputStream.read(buffer, 0, 1); //totalToRead);

                if (buffer[0] == delimByte) {
                    nfieldsRead++;
                }
                // append to the buffer
                value.append(buffer, 0, read);
                totalRead += read;


             //   newSize = in.readLine(v, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, splitEnd - pos), maxLineLength));
             //   nfieldsRead += StringUtils.split(new String(v.getBytes()), '~').length;

             //   value.append(v.getBytes(),0, v.getLength());
                //value.append(endline.getBytes(),0, endline.getLength());
                if (read == 0) {
                    break;
                }
                pos += read;
            }
        }
        if (read == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}
 /*   @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        int totalRead = 0; // total bytes read
        if (currentPosition < splitEnd) {
            while (nfieldsRead < nfields) {
                byte[] buffer = new byte[1];
                int read = this.fileInputStream.read(buffer, 0, 1); //totalToRead);

                if (buffer[0] == delimByte) {
                    nfieldsRead++;
                }
                // append to the buffer
                recordValue.append(buffer, 0, read);
                totalRead += read;
            }
            // update our current position and log the input bytes
            currentPosition = currentPosition + totalRead;//recordLength;
            inputByteCounter.increment(totalRead);

            LOG.info("VALUE=|"+fileInputStream.getPos()+"|"+currentPosition+"|"+splitEnd+"|" + totalRead + "|"+recordValue.toString());
            return true;
        }

        // nothing more to read....
        return false;
    }

}
   */
