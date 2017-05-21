package edu.neu.ir;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by paulomimahidharia on 5/21/17.
 */
public class Extra {

    private final static String DATA_DIR = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static int NUM_OF_DOCS = 84679;
    private final static String DOC_PATTERN = "<DOC>\\s(.+?)</DOC>";
    private final static String DOCNO_PATTERN = "<DOCNO>(.+?)</DOCNO>";
    private final static String TEXT_PATTERN = "<TEXT>\\s(.+?)</TEXT>";

    public static void main(String args[]) throws IOException {

        File dir = new File(DATA_DIR);
        Map<String, String> docnoTextMap = new HashMap<String, String>();
        PrintWriter writer = new PrintWriter("okapi_TF.txt", "UTF-8");

        // Get list of relevant files
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("ap89");
            }
        });


        int DOCcount = 0;
        int DOCLength = 0;



        for (File file : files) {
            // Read file as a String
            File mFile = new File(file.getPath());
            String str = FileUtils.readFileToString(mFile);

            // Extract DOC
            Pattern DOCpattern = Pattern.compile(DOC_PATTERN, Pattern.DOTALL);
            Matcher DOCmatcher = DOCpattern.matcher(str);


            while (DOCmatcher.find()) {

                DOCcount++;


                // Save DOC
                String doc = DOCmatcher.group(1);


                // Extract DOCNO
                final Pattern DOCNOPattern = Pattern.compile(DOCNO_PATTERN);
                final Matcher DOCNOMAtcher = DOCNOPattern.matcher(doc);

                String docNo = "";
                if (DOCNOMAtcher.find()) {
                    docNo = DOCNOMAtcher.group(1).trim();
                    // System.out.println(docNo);
                }

                // Extract TEXT
                Pattern TEXTPattern = Pattern.compile(TEXT_PATTERN, Pattern.DOTALL);
                Matcher TEXTMatcher = TEXTPattern.matcher(doc);

                String text = "";

                while (TEXTMatcher.find()) {
                    text = text.concat(TEXTMatcher.group(1));
                }
                // System.out.println(text);

                // Create Hash Entry
                text = text.replaceAll("\n", " ");


                docnoTextMap.put(docNo, text);
            }


        }

        long wordCount = 0;

        for(String key : docnoTextMap.keySet()) {
            String text = docnoTextMap.get(key);
            String[] words = text.trim().split(" ");
            wordCount = wordCount + words.length;

        }

        System.out.println("NO OF DOCS:" + DOCcount);
        System.out.println("DOC LENGTH:" + wordCount);
    }
}
