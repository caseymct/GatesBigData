package model;

import GatesBigData.utils.Constants;
import GatesBigData.utils.SolrUtils;
import GatesBigData.utils.Utils;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.solr.common.SolrDocument;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SolrRecord {

    private String id           = null;
    private byte[] content      = new byte[0];
    private String contentType  = null;
    private Metadata metadata   = null;
    private String fileName     = "";

    public SolrRecord() {
        metadata = new Metadata();
    }

    public SolrRecord(SolrDocument doc, List<String> fields) {

        JSONObject jsonObject = new JSONObject();
        for(String s : fields) {
            if (doc.containsKey(s)) {
                jsonObject.put(s, doc.get(s));
            }
        }
        String contentStr = jsonObject.toString(3).replaceAll("\n", Constants.DEFAULT_NEWLINE);
        content = contentStr.getBytes();
        contentType = SolrUtils.getDocumentContentType(doc);
        id  = SolrUtils.getFieldStringValue(doc, Constants.SOLR_ID_FIELD_NAME, "not_found");
        fileName = new File(id).getName();
    }

    public SolrRecord(SolrDocument doc) {
        this(doc, new ArrayList<String>(doc.getFieldNames()));
    }

    public SolrRecord(Content recordContent) {
        this(recordContent, (recordContent == null) ? "" : recordContent.getUrl());
    }

    public SolrRecord(Content recordContent, String fileName) {
        if (recordContent != null) {
            this.content = recordContent.getContent();
            this.contentType = recordContent.getContentType();
            this.id = recordContent.getUrl();
            this.fileName = new File(fileName).getName();
        }
    }

    public SolrRecord(String id, byte[] content, String contentType, String fileName, Metadata metadata) {
        if (id == null)
            throw new IllegalArgumentException("null url");
        if (content == null)
            throw new IllegalArgumentException("null content");
        if (contentType == null)
            throw new IllegalArgumentException("null contentType");

        this.content = content;
        this.contentType = contentType;
        this.metadata = metadata;
        this.fileName = fileName;
    }

    public void ifRecordIsJSONChangeToText() {
        if (Utils.fileHasExtension(this.fileName, Constants.JSON_FILE_EXT)) {
            this.fileName = Utils.changeFileExtension(this.fileName, Constants.TEXT_FILE_EXT, false);
            this.contentType = Constants.TEXT_CONTENT_TYPE;
        }
    }

    public final void write(DataOutput out) throws IOException {
        Text.writeString(out, id); // write url

        out.writeInt(content.length); // write content
        out.write(content);

        Text.writeString(out, contentType); // write contentType

        metadata.write(out); // write metadata
    }

    public String getId() {
        return id;
    }

    /** The binary content retrieved. */
    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    /** The media type of the retrieved content.
     * @see <a href="http://www.iana.org/assignments/media-types/">
     *      http://www.iana.org/assignments/media-types/</a>
     */
    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    /** Other protocol-specific data. */
    public Metadata getMetadata() {
        return metadata;
    }

    /** Other protocol-specific data. */
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public String getFileName() {
        return this.fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("id: ").append(id).append("\n");
        buffer.append("contentType: ").append(contentType).append("\n");
        buffer.append("metadata: ").append(metadata).append("\n");
        buffer.append("Content:\n");
        buffer.append(Utils.getUTF8String(content)); // try default encoding

        return buffer.toString();
    }
}
