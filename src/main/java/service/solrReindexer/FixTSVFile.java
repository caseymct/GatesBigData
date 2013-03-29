import java.io.*;
import java.util.*;

public class FixTSVFile {
    public static String lineSeparator           = System.getProperty("line.separator");
    public static byte[] newline                 = lineSeparator.getBytes();

    public static String ARG_KEY_PRINT_N_LINES   = "printnlines";
    public static String ARG_KEY_FILENAME        = "filename";
    public static String ARG_KEY_DELIMITER       = "delimiter";
    public static String ARG_KEY_CHECK_FILE      = "check";

    public static String delimiter               = "~";
    public static File fileToRead;
    public static boolean check                  = false;
    public static boolean printNLines            = false;
    public static int nLinesToPrint              = 0;

    public static List<String> getTokens(String d, String delimiter) {
        List<String> tokens = new ArrayList<String>();

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

    public static void writeToFile(String line, FileOutputStream out) throws IOException {
        byte[] allfieldsBytes = line.getBytes();
        byte[] b = new byte[allfieldsBytes.length + newline.length];
        System.arraycopy(allfieldsBytes, 0, b, 0, allfieldsBytes.length);
        System.arraycopy(newline, 0, b, allfieldsBytes.length, newline.length);
        out.write(b);
    }

    public static String join(List<String> list1, String delimiter) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < list1.size(); i++) {
            sb.append(list1.get(i));
            if (i < list1.size() - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public static void printNLines(List<String> fields, List<Integer> indicesToRemove, BufferedReader br) throws IOException {
        String line;
        int i = 1;
        TreeMap<String, List<String>> fieldToValues = new TreeMap<String, List<String>>();

        while (i < nLinesToPrint && (line = br.readLine()) != null) {
            i++;
            List<String> lineFields = removeIndices(indicesToRemove, getTokens(line, delimiter));

            for(int j = 0; j < lineFields.size(); j++) {
                String field = fields.get(j);
                List<String> vals = fieldToValues.containsKey(field) ? fieldToValues.get(field) : new ArrayList<String>();
                vals.add(lineFields.get(j));
                fieldToValues.put(field, vals);
            }
        }

        for(Map.Entry<String, List<String>> entry : fieldToValues.entrySet()) {
            System.out.format("%-5s:%10s\n", entry.getKey(), join(entry.getValue(), "\t"));
        }
    }

    public static void checkFile(int nFields, BufferedReader br) throws IOException {
        System.out.println("Checking modified file... ");
        String line;
        int i = 1;
        List<String> mismatches = new ArrayList<String>();

        while ((line = br.readLine()) != null) {
            i++;
            int nFieldsPerLine = getTokens(line, delimiter).size();
            if (nFieldsPerLine != nFields) {
                mismatches.add(i + " mismatch; fieldsPerLine: " + nFieldsPerLine + ", fields: " + nFields);
            }
        }

        if (mismatches.size() == 0) {
            System.out.println("File check successful.");
        } else {
            System.out.println(join(mismatches, "\n"));
        }
    }


    public static String getOutputFileName(String fileName) {
        return fileName + ".modified";
    }

    public static void process(String fileName) throws IOException {
        DataInputStream in = new DataInputStream(new FileInputStream(fileName));
        BufferedReader br  = new BufferedReader(new InputStreamReader(in));

        String fullLine = "", line = br.readLine();
        List<String> fields = getTokens(line, delimiter);
        int nFieldsPerLine, nFields = fields.size();

        List<Integer> dupIndices = initDuplicateIndices(fields);
        fields = removeIndices(dupIndices, fields);

        if (check) {
            checkFile(fields.size(), br);
        } else if (printNLines) {
            printNLines(fields, dupIndices, br);
        } else {
            FileOutputStream out = new FileOutputStream(getOutputFileName(fileName));
            writeToFile(join(fields, delimiter), out);

            while ((line = br.readLine()) != null) {
                List<String> tokens = getTokens(line, delimiter);
                nFieldsPerLine = tokens.size();
                fullLine = line;

                while (nFieldsPerLine < nFields) {
                    line = br.readLine();
                    tokens.addAll(getTokens(line, delimiter));
                    nFieldsPerLine = tokens.size();
                    fullLine += line;
                }

                List<String> allTokens = removeIndices(dupIndices, getTokens(fullLine, delimiter));
                writeToFile(join(allTokens, delimiter), out);
            }

            out.flush();
            out.close();
        }

        in.close();
        br.close();
    }

    public static List<Integer> initDuplicateIndices(List<String> fields) {
        HashMap<String, List<Integer>> dupIndices = new HashMap<String, List<Integer>>();
        List<Integer> allDups = new ArrayList<Integer>();

        for(String dup : findDups(fields)) {
            int index = fields.indexOf(dup);

            for(int j = index + 1; j < fields.size(); j++) {
                String field = fields.get(j);
                if (field.equals(dup)) {
                    List<Integer> dups = dupIndices.containsKey(field) ? dupIndices.get(field) : new ArrayList<Integer>();
                    dups.add(j);
                    allDups.add(j);
                    dupIndices.put(field, dups);
                }
            }
        }

        for(Map.Entry<String, List<Integer>> entry : dupIndices.entrySet()) {
            System.out.print("Duplicates of " + entry.getKey() + " are at ");
            for(Object val : entry.getValue()) {
                System.out.print(val + " ");
            }
            System.out.println();
        }
        Collections.reverse(allDups);
        return allDups;
    }

    public static void parseArgs(String[] args) {
        if (args.length == 1 && args[0].equals("-")) {
            System.out.println("Usage: sudo java FixTSVFile filename=filename");
            System.out.println("optional: delimiter=String printNLines=int removedups=dup1,dup2,...,dupN check=boolean");
            System.exit(0);
        }

        for(String arg : args) {
            String[] argArray = arg.split("=");
            String key = argArray[0], value = argArray[1];

            if (key.equals(ARG_KEY_DELIMITER)) {
                delimiter = value;
            } else if (key.equals(ARG_KEY_FILENAME)) {
                fileToRead = new File(value);
                boolean exists = fileToRead.exists(), canRead = fileToRead.canRead();
                if (!exists || !canRead) {
                    System.err.println("File error with " + fileToRead.getName() + ": exists: " + exists + ", canRead: " + canRead);
                    System.exit(1);
                }
            } else if (key.equals(ARG_KEY_PRINT_N_LINES)) {
                printNLines = true;
                nLinesToPrint = Integer.parseInt(value);
            } else if (key.equals(ARG_KEY_CHECK_FILE)) {
                check = true;
            }
        }
    }

    public static Set<String> findDups(List<String> fields) {
        Set<String> duplicates = new HashSet<String>();
        Set<String> set = new HashSet<String>();

        for (String field : fields) {
            if (!set.add(field)) {
                duplicates.add(field);
            }
        }
        return duplicates;
    }

    public static void printIndices(List<Integer> indices, String header) {
        System.out.println(header);
        for(int i : indices) {
            System.out.print(i + " ");
        }
        System.out.println();
    }

    public static List<String> removeIndices(List<Integer> indices, List<String> fields) {
        for(int index: indices) {
            fields.remove(index);
        }
        return fields;
    }

    public static void main(String[] args) {
        parseArgs(args);

        try {
            process(fileToRead.getName());

            File modifiedFile = new File(getOutputFileName(fileToRead.getName()));
            if (modifiedFile.exists()) {
                check = true;
                process(modifiedFile.getName());
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

}
