package service.solrReindexer;

import java.io.*;

public class FixTSVFile {
    public static int nfields = 74;

    public static void main(String[] args) {
        File fileName = new File(args[0]);
        byte[] newline = "\n".getBytes();

        try {
            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            FileOutputStream out = new FileOutputStream(fileName.getName() + ".modified");

            String allfields = "", line = "";
            int nFieldsPerLine = 0;

            while ((line = br.readLine()) != null) {
                nFieldsPerLine = line.split("~").length;
                allfields = line;

                while (nFieldsPerLine < nfields) {
                    line = br.readLine();
                    nFieldsPerLine += line.split("~").length;
                    allfields += line;
                }
                byte[] allfieldsBytes = allfields.getBytes();
                byte[] b = new byte[allfieldsBytes.length + newline.length];
                System.arraycopy(allfieldsBytes, 0, b, 0, allfieldsBytes.length);
                System.arraycopy(newline, 0, b, allfieldsBytes.length, newline.length);
                out.write(b);
            }
            in.close();
            out.flush();
            out.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

}
