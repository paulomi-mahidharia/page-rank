package edu.neu.utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.simple.parser.ParseException;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.neu.utility.JsonProcessing.parseStringToJson;

/**
 * Created by paulomimahidharia on 5/29/17.
 */
public class FileParser {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";

    private final static String DOC_PATTERN = "<DOC>\\s(.+?)</DOC>";
    private final static String DOCNO_PATTERN = "<DOCNO>(.+?)</DOCNO>";
    private final static String DATA_DIR = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection";


    public static List<String> getDocIds() throws IOException {

        List<String> docNames = new ArrayList<String>();
        File dir = new File(DATA_DIR);

        // Get list of relevant files
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("ap89");
            }
        });

        for (File file: files) {

            // Read file as a String
            File mFile = new File(file.getPath());
            String str = FileUtils.readFileToString(mFile);

            // Extract DOC
            Pattern DOCpattern = Pattern.compile(DOC_PATTERN, Pattern.DOTALL);
            Matcher DOCmatcher = DOCpattern.matcher(str);

            while (DOCmatcher.find()) {

                // Save DOC
                String doc = DOCmatcher.group(1);

                // Extract DOCNO
                final Pattern DOCNOPattern = Pattern.compile(DOCNO_PATTERN);
                final Matcher DOCNOMAtcher = DOCNOPattern.matcher(doc);

                //String docNo = "";
                if(DOCNOMAtcher.find()) {
                    String docNo = DOCNOMAtcher.group(1).trim();
                    docNames.add(docNo);
                }

            }
        }
        return docNames;
    }


    public static Map<String, Double> getDocIdsWithLength() throws IOException, ParseException {

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Map<String, Double> docNames = new HashMap<String, Double>();
        File dir = new File(DATA_DIR);

        // Get list of relevant files
        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.startsWith("ap89");
            }
        });

        for (File file: files) {

            // Read file as a String
            File mFile = new File(file.getPath());
            String str = FileUtils.readFileToString(mFile);

            // Extract DOC
            Pattern DOCpattern = Pattern.compile(DOC_PATTERN, Pattern.DOTALL);
            Matcher DOCmatcher = DOCpattern.matcher(str);

            while (DOCmatcher.find()) {

                // Save DOC
                String doc = DOCmatcher.group(1);

                // Extract DOCNO
                final Pattern DOCNOPattern = Pattern.compile(DOCNO_PATTERN);
                final Matcher DOCNOMAtcher = DOCNOPattern.matcher(doc);

                //String docNo = "";
                if(DOCNOMAtcher.find()) {
                    String docID = DOCNOMAtcher.group(1).trim();

                    //Get Doc length
                    JsonObject vocabularyObj = Json.createObjectBuilder()
                            .add("query", Json.createObjectBuilder()
                                    .add("match_phrase", Json.createObjectBuilder()
                                            .add("docNo", docID)))
                            .add("script_fields", Json.createObjectBuilder()
                                    .add("doc_length", Json.createObjectBuilder()
                                            .add("script", Json.createObjectBuilder()
                                                    .add("inline", "doc['text'].values.size()"))))
                            .build();

                    HttpEntity docLengthEntity = new NStringEntity(vocabularyObj.toString(), ContentType.APPLICATION_JSON);
                    Response docLengthResponse = restClient.performRequest("GET", "/ap89_dataset/_search/", Collections.<String, String>emptyMap(), docLengthEntity);
                    ObjectNode docLengthObject = parseStringToJson(EntityUtils.toString(docLengthResponse.getEntity()));


                    JsonNode hits = docLengthObject.get("hits").get("hits");
                    double docLength = 0.0;

                    for (JsonNode hit : hits) {
                        docLength = Double.parseDouble(hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim());
                    }

                    docNames.put(docID, docLength);
                }

            }
        }
        return docNames;
    }
}
