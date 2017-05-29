package edu.neu.beans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.neu.beans.TermHitsInfo;
import edu.neu.beans.TermTFBean;
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
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.neu.utility.JsonProcessing.parseStringToJson;
import static edu.neu.utility.QueryProcessing.getStopWords;
import static edu.neu.utility.QueryProcessing.removeStopWordsFromQuery;
import static edu.neu.utility.SortMap.sortMapByScoreDouble;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class JMSmoothing2 {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String OUTPUT = "JMSmoothing.txt";
    private final static String DATA_DIR = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection";
    private final static String DOC_PATTERN = "<DOC>\\s(.+?)</DOC>";
    private final static String DOCNO_PATTERN = "<DOCNO>(.+?)</DOCNO>";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";
    private static Map<String, TermHitsInfo> termHitsMap = null;
    private static Set<String> stopWords = null;

    public static void main(String[] args) throws IOException, ParseException {

        File output = new File(OUTPUT);
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));

        /**
         * Get all Document ids
         */
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

        Map<String, Map<String, TermTFBean>> docTermTFMap = new HashMap<String, Map<String, TermTFBean>>();


        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        System.out.println("NUMBER OF DOCS : "+docNames.size());

        Map<String, Map<String, TermHitsInfo>> termMap = new HashMap<String, Map<String, TermHitsInfo>>();

        File queryFile = new File(QUERY_FILE);
        BufferedReader br = new BufferedReader(new FileReader(queryFile));
        String query = null;
        while ((query = br.readLine()) != null) {

            //for each query
            if (query.length() <= 3) {
                break;
            }

            String queryNo = query.substring(0, 3).replace(".", "").trim();
            System.out.println("QUERY NO : " + queryNo);

            query = query.substring(5).trim();

            stopWords = getStopWords();
            StringBuffer cleanQuery = removeStopWordsFromQuery(query, stopWords);
            String[] cleanQueryWords = cleanQuery.toString().trim().split(" ");



            for(String word : cleanQueryWords) {

                if(termMap.containsKey(word)) continue;

                // Get stem for current word
                JsonObject stemObj = Json.createObjectBuilder()
                        .add("analyzer", "my_english")
                        .add("text", word)
                        .build();

                HttpEntity stemEntity = new NStringEntity(stemObj.toString(), ContentType.APPLICATION_JSON);
                Response response = restClient.performRequest("GET", "ap89_dataset/_analyze", Collections.<String, String>emptyMap(), stemEntity);

                ObjectNode stemResponse = parseStringToJson(EntityUtils.toString(response.getEntity()));
                String stem = "";

                for (JsonNode tokenObj : stemResponse.get("tokens")) {
                    stem = tokenObj.get("token").asText().replace("\"", "").replace("\'", "\\'");
                }

                System.out.println(stem);

                // Get TF for given word
                JsonObject TFObject = Json.createObjectBuilder()
                        .add("size", 10000)
                        .add("_source", true)
                        .add("query", Json.createObjectBuilder()
                                .add("match", Json.createObjectBuilder()
                                        .add("text", word)))
                        .add("script_fields", Json.createObjectBuilder()
                                .add("index_tf", Json.createObjectBuilder()
                                        .add("script", Json.createObjectBuilder()
                                                .add("lang", "groovy")
                                                .add("inline", "_index['text']['" + stem + "'].tf()")))
                                .add("doc_length", Json.createObjectBuilder()
                                        .add("script", Json.createObjectBuilder()
                                                .add("inline", "doc['text'].values.size()"))))
                        .build();

                HttpEntity TFEntity = new NStringEntity(TFObject.toString(), ContentType.APPLICATION_JSON);
                Response TFResponse = restClient.performRequest("GET", "/ap89_dataset/_search/?scroll=1m", Collections.<String, String>emptyMap(), TFEntity);
                ObjectNode TFJSON = parseStringToJson(EntityUtils.toString(TFResponse.getEntity()));

                JsonNode hits = TFJSON.get("hits").get("hits");
                double maxScore = TFJSON.get("hits").get("max_score").asDouble();
                termHitsMap = new HashMap<String, TermHitsInfo>();

                for (JsonNode hit : hits) {

                    updateMap(hit, maxScore);
                }

                String scrollId = "";
                long numberOfHits = TFJSON.get("hits").get("total").asLong();
                if (numberOfHits > 10000) {

                    scrollId = TFJSON.get("_scroll_id").asText();

                    while(numberOfHits > 10000){

                        JsonObject TFScrollObject = Json.createObjectBuilder()
                                .add("scroll", "1m")
                                .add("scroll_id", scrollId)
                                .build();
                        HttpEntity TFScrollEntity = new NStringEntity(TFScrollObject.toString(), ContentType.APPLICATION_JSON);
                        Response TFScrollResponse = restClient.performRequest("POST", "/_search/scroll", Collections.<String, String>emptyMap(), TFScrollEntity);
                        ObjectNode TFScrollJSON = parseStringToJson(EntityUtils.toString(TFScrollResponse.getEntity()));

                        JsonNode scrollHits = TFScrollJSON.get("hits").get("hits");

                        for (JsonNode hit : scrollHits) {

                            updateMap(hit, maxScore);
                        }

                        scrollId = TFScrollJSON.get("_scroll_id").asText();
                        numberOfHits = numberOfHits - 10000;
                    }
                }

                termMap.put(word, termHitsMap);

            }
        }

        br.close();


        long sum_doc_freq = 13944841;

        // Read and store StopList terms


        queryFile = new File(QUERY_FILE);
        br = new BufferedReader(new FileReader(queryFile));

        while ((query = br.readLine()) != null) {

            HashMap<String, Double> JMSmoothingMap = new HashMap<String, Double>();
            HashMap<String, Double> SortedMap;

            //for each query
            if (query.length() <= 3) {
                break;
            }

            String queryNo = query.substring(0, 3).replace(".", "").trim();
            System.out.println("QUERY NO : " + queryNo);

            query = query.substring(5).trim();

            StringBuffer cleanQuery = removeStopWordsFromQuery(query, stopWords);
            String[] cleanQueryWords = cleanQuery.toString().trim().split(" ");

            for(String word : cleanQueryWords) {

                System.out.println(word);

                Map<String, TermHitsInfo> docMapForWord = termMap.get(word);
                double maxScore = 0.0;
                if(docMapForWord.size() > 0){
                    maxScore = docMapForWord.entrySet().iterator().next().getValue().getMaxScore();
                }else{
                    System.out.println("IN else");
                    maxScore = 0.0;
                }


                for(String docName: docNames){

                    //Get Doc Length
                    Float JM;
                    if(docMapForWord.containsKey(docName)){

                        TermHitsInfo t = docMapForWord.get(docName);
                        int TFWD = t.getTF();
                        int docLength = t.getDocLength();


                        Float term1 = (float) ((0.6 * TFWD / docLength) + (0.4 * (maxScore - TFWD) / (sum_doc_freq - docLength)));
                        JM = (float) (Math.log(term1));

                    }else{

                        int TFWD =  0;

                        //Get Doc length
                        JsonObject vocabularyObj = Json.createObjectBuilder()
                                .add("query", Json.createObjectBuilder()
                                        .add("match_phrase", Json.createObjectBuilder()
                                                .add("docNo", docName)))
                                .add("script_fields", Json.createObjectBuilder()
                                        .add("doc_length", Json.createObjectBuilder()
                                                .add("script", Json.createObjectBuilder()
                                                        .add("inline", "doc['text'].values.size()"))))
                                .build();

                        HttpEntity docLengthEntity = new NStringEntity(vocabularyObj.toString(), ContentType.APPLICATION_JSON);
                        Response docLengthResponse = restClient.performRequest("GET", "/ap89_dataset/_search/", Collections.<String, String>emptyMap(), docLengthEntity);
                        ObjectNode docLengthObject = parseStringToJson(EntityUtils.toString(docLengthResponse.getEntity()));


                        JsonNode hits = docLengthObject.get("hits").get("hits");
                        int docLength = 0;

                        for(JsonNode hit: hits){
                            docLength = Integer.parseInt(hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim());
                        }



                        Float term1 = (float) ((0.6 * TFWD / docLength) + (0.4 * (maxScore - TFWD) / (sum_doc_freq - docLength)));
                        JM = (float) (Math.log(term1));

                    }

                    JMSmoothingMap.put(docName,
                            (JMSmoothingMap.get(docName) == null ? JM : JMSmoothingMap.get(docName) + JM));

                }
            }

            System.out.println("SIZE : " + JMSmoothingMap.size());
            SortedMap = sortMapByScoreDouble(JMSmoothingMap);
            System.out.println("SORTED SIZE : " + SortedMap.size());
            int rank = 0;

            for (Map.Entry m1 : SortedMap.entrySet()) {

                if (rank < 1000) {
                    rank = rank + 1;
                    writer.write(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " JM\n");
                } else
                    break;
            }
            SortedMap.clear();
            JMSmoothingMap.clear();
        }
        writer.close();
        br.close();

        System.out.println("DONE");

    }

    private static void updateMap(JsonNode hit, double maxScore) {

        String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
        int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

        String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
        int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

        String docNo = hit.get("_source").get("docNo").asText();

        termHitsMap.put(docNo, new TermHitsInfo(TFWD, docLegth, maxScore));

    }

}
