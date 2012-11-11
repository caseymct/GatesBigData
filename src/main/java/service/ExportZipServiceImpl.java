package service;


import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.zip.ZipOutputStream;

public class ExportZipServiceImpl implements ExportZipService {

    private FileOutputStream fileOutputStream;
    private ZipOutputStream zipOutputStream;

    private static final Logger logger = Logger.getLogger(ExportZipServiceImpl.class);
    private HDFSService hdfsService;
    private SolrService solrService;
    private CoreService coreService;

    @Autowired
    public void setServices(HDFSService hdfsService, SolrService solrService, CoreService coreService) {
        this.hdfsService = hdfsService;
        this.solrService = solrService;
        this.coreService = coreService;
    }

    public void initializeZipfile(String zipfileName) {
        try {
            fileOutputStream = new FileOutputStream(zipfileName);
            zipOutputStream = new ZipOutputStream(fileOutputStream);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        }
    }


    public void exportHeaderData(long numDocs, String query, String fq, String coreName, final Writer writer, String delimiter, String newLine) {


        try {
            writer.append("# Found").append(delimiter).append("Solr Query").append(delimiter);
            writer.append("Core name").append(delimiter).append("Filter Query").append(delimiter);
            writer.append(newLine);

            writer.append(Long.toString(numDocs)).append(delimiter).append(query).append(delimiter);
            writer.append(coreName).append(delimiter).append(fq).append(delimiter);
            writer.append(newLine).append(newLine);

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void closeWriters(final Writer writer) {
        try {
            writer.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeEmptyResultSet(final Writer writer) {
        try {
            writer.append("No search results.");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
