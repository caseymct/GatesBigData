package service.solrReindexer;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SolrIndexAction implements Writable {

    public static final byte ADD = 0;
    public static final byte DELETE = 1;

    public SolrInputDocument doc = null;
    public byte action = ADD;

    public SolrIndexAction(SolrInputDocument doc, byte action) {
        this.doc = doc;
        this.action = action;
    }

    public void readFields(DataInput in) throws IOException {
        action = in.readByte();
        SolrInputDocument doc = new SolrInputDocument();
        //doc.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        out.write(action);
        // doc.write(out);
    }
}
