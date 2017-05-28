package edu.neu.ir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import static edu.neu.utility.JsonProcessing.parseStringToJson;
import static edu.neu.utility.QueryProcessing.getStopWords;
import static edu.neu.utility.QueryProcessing.removeStopWordsFromQuery;
import static edu.neu.utility.SortMap.sortMapByScore;

/**
 * Created by paulomimahidharia on 5/20/17.
 */
public class okapiTF {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";
    private static long DOCCount = 0;
    private final static String OUTPUT = "okapiTF.txt";
    private static HashMap<String, Float> okapiTFMAP = null;

    public static void main(String args[]) throws IOException, ParseException {

        File queryFile = new File(QUERY_FILE);
        File okapiTFOutput = new File(OUTPUT);

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Response countResponse = restClient.performRequest("GET", "/ap_dataset/document/_count");
        ObjectNode countJson = parseStringToJson(EntityUtils.toString(countResponse.getEntity()));
        DOCCount = Long.parseLong(countJson.get("count").toString());

        double docAverage = (double) 20976545 / 84612;
        BufferedWriter writer = new BufferedWriter(new FileWriter(okapiTFOutput));

        // Read and store StopList terms
        Set<String> stopWords = getStopWords();
        System.out.println(stopWords.size());

        BufferedReader br = new BufferedReader(new FileReader(queryFile));
        String query = null;
        while ((query = br.readLine()) != null) {

            //for each query
            if (query.length() <= 3) {
                break;
            }

            okapiTFMAP = new HashMap<String, Float>();
            HashMap<String, Float> SortedMap = new HashMap<String, Float>();

            String queryNo = query.substring(0, 3).replace(".", "").trim();
            query = query.substring(5).trim();

            StringBuffer cleanQuery = removeStopWordsFromQuery(query, stopWords);
            System.out.println("NEW : " + cleanQuery.toString());

            //Get each word in the query
            String[] cleanQueryWords = cleanQuery.toString().trim().split(" ");
            for (String word : cleanQueryWords) {

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

                for (JsonNode hit : hits) {

                    updateMap(hit, docAverage);
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

                            updateMap(hit, docAverage);
                        }

                        scrollId = TFScrollJSON.get("_scroll_id").asText();
                        numberOfHits = numberOfHits - 10000;
                    }
                }
            }

            SortedMap = sortMapByScore(okapiTFMAP);
            int rank = 0;

            for (Map.Entry m1 : SortedMap.entrySet()) {

                if (rank < 1000) {
                    rank = rank + 1;
                    writer.write(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " OkapiTF\n");
                } else
                    break;
            }
            SortedMap.clear();
            okapiTFMAP.clear();
        }
        writer.close();
        restClient.close();
        br.close();
    }

    private static void updateMap(JsonNode hit, double docAverage){

        String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
        int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

        String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
        int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

        String docNo = hit.get("_source").get("docNo").asText();

        Float OkapiTFWD = (float) (TFWD / (TFWD + 0.5 + (1.5 * (docLegth / docAverage))));
        okapiTFMAP.put(docNo,
                okapiTFMAP.get(docNo) == null ? OkapiTFWD : okapiTFMAP.get(docNo) + OkapiTFWD);
    }
}
