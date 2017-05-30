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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static edu.neu.utility.JsonProcessing.parseStringToJson;
import static edu.neu.utility.QueryProcessing.getStopWords;
import static edu.neu.utility.QueryProcessing.removeStopWordsFromQuery;
import static edu.neu.utility.SortMap.sortMapByScore;

/**
 * Created by paulomimahidharia on 5/20/17.
 */
public class TF_IDF {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";

    private final static String OUTPUT = "TF_IDF.txt";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";

    private static long DOCCount = 0;
    private static HashMap<String, Float> TFIDFMap = null;

    public static void main(String args[]) throws IOException, ParseException {

        File queryFile = new File(QUERY_FILE);
        File okapiTFOutput = new File(OUTPUT);

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Response countResponse = restClient.performRequest("GET", "/ap_dataset/document/_count");
        ObjectNode countJson = parseStringToJson(EntityUtils.toString(countResponse.getEntity()));
        DOCCount = Long.parseLong(countJson.get("count").toString());
        System.out.println(DOCCount);

        double docAverage = (double) 20976545 / 84612;
        BufferedWriter writer = new BufferedWriter(new FileWriter(okapiTFOutput));

        // Read and store StopList terms
        Set<String> stopWords = getStopWords();
        System.out.println(stopWords.size());

        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String query;
        while ((query = reader.readLine()) != null) {

            //for each query
            if (query.length() <= 3) {
                break;
            }

            TFIDFMap = new HashMap<String, Float>();
            HashMap<String, Float> SortedMap;

            String queryNo = query.substring(0, 3).replace(".", "").trim();
            query = query.substring(5).trim();
            StringBuffer cleanQuery = removeStopWordsFromQuery(query, stopWords);


            //Get each word in the query
            String[] cleanQueryWords = cleanQuery.toString().trim().split(" ");
            for (String word : cleanQueryWords) {

                word = word.trim();

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
                    stem = tokenObj.get("token").asText().replace("\"", "").replace("\'", "\\'").trim();
                }

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
                long numberOfHits = TFJSON.get("hits").get("total").asLong();
                System.out.println("HITS : " + numberOfHits);

                for (JsonNode hit : hits) {

                    updateMap(hit, docAverage, numberOfHits);
                }

                String scrollId = "";
                long totalHits = numberOfHits;

                if (totalHits > 10000) {

                    scrollId = TFJSON.get("_scroll_id").asText();
                    while (totalHits > 10000) {

                        JsonObject TFIDFScrollObject = Json.createObjectBuilder()
                                .add("scroll", "1m")
                                .add("scroll_id", scrollId)
                                .build();

                        HttpEntity TFIDFScrollEntity = new NStringEntity(TFIDFScrollObject.toString(), ContentType.APPLICATION_JSON);
                        Response TFIDFScrollResponse = restClient.performRequest("POST", "/_search/scroll", Collections.<String, String>emptyMap(), TFIDFScrollEntity);
                        ObjectNode TFIDFScrollJSON = parseStringToJson(EntityUtils.toString(TFIDFScrollResponse.getEntity()));

                        JsonNode scrollHits = TFIDFScrollJSON.get("hits").get("hits");

                        for (JsonNode hit : scrollHits) {

                            updateMap(hit, docAverage, numberOfHits);
                        }

                        scrollId = TFIDFScrollJSON.get("_scroll_id").asText();
                        totalHits = totalHits - 10000;
                    }
                }
            }

            SortedMap = sortMapByScore(TFIDFMap);
            int rank = 0;

            for (Map.Entry m1 : SortedMap.entrySet()) {

                if (rank < 1000) {
                    rank = rank + 1;
                    writer.write(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " TF_IDF\n");
                } else
                    break;
            }
            SortedMap.clear();
            TFIDFMap.clear();
        }
        writer.close();
        restClient.close();
        reader.close();
    }

    private static void updateMap(JsonNode hit, double docAverage, long numberOfHits) {

        String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
        int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

        String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
        int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

        String docNo = hit.get("_source").get("docNo").asText();
        Float OkapiTFWD = (float) (TFWD / (TFWD + 0.5 + (1.5 * (docLegth / docAverage))));

        Float TF_IDFWD = (float) (OkapiTFWD * (Math.log(DOCCount / numberOfHits)));
        TFIDFMap.put(docNo,
                TFIDFMap.get(docNo) == null ? TF_IDFWD : TFIDFMap.get(docNo) + TF_IDFWD);
    }
}
