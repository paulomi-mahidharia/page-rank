package edu.neu.ir;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by paulomimahidharia on 5/16/17.
 */
public class Indexer {

    private final static String DATA_DIR = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection";
    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String CLUSTER_NAME = "elasticsearch";
    private final static String DOC_PATTERN = "<DOC>\\s(.+?)</DOC>";
    private final static String DOCNO_PATTERN = "<DOCNO>(.+?)</DOCNO>";
    private final static String TEXT_PATTERN = "<TEXT>\\s(.+?)</TEXT>";

    public static void main(String[] args) throws IOException {

        int DOCcount = 0;
        Map<String,String> docnoTextMap = new HashMap<String,String>();

        File dir = new File(DATA_DIR);

        // Get list of relevant files
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("ap89");
            }
        });

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();


        for (File file: files) {

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
                if(DOCNOMAtcher.find()) {
                    docNo = DOCNOMAtcher.group(1).trim();
                    // System.out.println(docNo);
                }

                // Extract TEXT
                Pattern TEXTPattern = Pattern.compile(TEXT_PATTERN, Pattern.DOTALL);
                Matcher TEXTMatcher = TEXTPattern.matcher(doc);

                String text = "";

                while (TEXTMatcher.find())
                {
                    text = text.concat(TEXTMatcher.group(1));
                }
                // System.out.println(text);

                // Create Hash Entry
                text = text.replaceAll("\n", " ");

                docnoTextMap.put(docNo, text);
            }
        }

        for(Map.Entry map: docnoTextMap.entrySet()){

            JSONObject obj = new JSONObject();
            obj.put("docNo", map.getKey());
            obj.put("text", map.getValue());

            HttpEntity entity = new NStringEntity(obj.toJSONString(), ContentType.APPLICATION_JSON);

            Response response = restClient.performRequest("PUT","ap_dataset/document/"+map.getKey(), Collections.<String, String>emptyMap(), entity);
            //System.out.println(EntityUtils.toString(response.getEntity()));
        }

        System.out.println("DONE");

        restClient.close();
    }
}
