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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.neu.utility.JsonProcessing.parseStringToJson;
import static edu.neu.utility.QueryProcessing.getStopWords;
import static edu.neu.utility.QueryProcessing.removeStopWordsFromQuery;
import static edu.neu.utility.SortMap.sortMapByScore;

/**
 * Created by paulomimahidharia on 5/20/17.
 */
public class OkapiBM25 {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";

    private final static float b = (float) 0.75;
    private final static float k1 = (float) 1.2;
    private final static float k2 = (float) 100;
    private final static double docAverage = (double) 20976545 / 84612;

    private final static String OUTPUT = "OkapiBM25.txt";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";

    private static long DOCCount = 0;
    private static HashMap<String, Float> okapiBM25Map = null;

    public static void main(String args[]) throws IOException, ParseException {

        File queryFile = new File(QUERY_FILE);
        File okapiTFOutput = new File(OUTPUT);

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Response countResponse = restClient.performRequest("GET", "/ap_dataset/document/_count");
        ObjectNode countJson = parseStringToJson(EntityUtils.toString(countResponse.getEntity()));
        DOCCount = Long.parseLong(countJson.get("count").toString());

        BufferedWriter writer = new BufferedWriter(new FileWriter(okapiTFOutput));

        // Read and store StopList terms
        Set<String> stopWords = getStopWords();

        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String query;
        while ((query = reader.readLine()) != null) {

            //for each query
            if (query.length() <= 3) {
                break;
            }

            okapiBM25Map = new HashMap<String, Float>();
            HashMap<String, Float> SortedMap;

            String queryNo = query.substring(0, 3).replace(".", "").trim();
            query = query.substring(5).trim();

            StringBuffer cleanQuery = removeStopWordsFromQuery(query, stopWords);

            //Get each word in the query
            String[] cleanQueryWords = cleanQuery.toString().trim().split(" ");
            for (String word : cleanQueryWords) {

                word = word.trim();

                int TFWQ = 0;
                Pattern p = Pattern.compile(word);
                Matcher m = p.matcher(cleanQuery);
                while (m.find()) {
                    TFWQ++;
                }

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

                long numberOfHits = TFJSON.get("hits").get("total").asLong();

                JsonNode hits = TFJSON.get("hits").get("hits");

                for (JsonNode hit : hits) {

                    updateMap(hit, numberOfHits, TFWQ);
                }

                String scrollId = "";
                long totalHits = numberOfHits;

                if (totalHits > 10000) {

                    scrollId = TFJSON.get("_scroll_id").asText();
                    while (totalHits > 10000) {

                        JsonObject okaiBM25ScrollObject = Json.createObjectBuilder()
                                .add("scroll", "1m")
                                .add("scroll_id", scrollId)
                                .build();

                        HttpEntity okapiBM25ScrollEntity = new NStringEntity(okaiBM25ScrollObject.toString(), ContentType.APPLICATION_JSON);
                        Response okapiBM25ScrollResponse = restClient.performRequest("POST", "/_search/scroll", Collections.<String, String>emptyMap(), okapiBM25ScrollEntity);
                        ObjectNode okapiBM25ScrollJSON = parseStringToJson(EntityUtils.toString(okapiBM25ScrollResponse.getEntity()));

                        JsonNode scrollHits = okapiBM25ScrollJSON.get("hits").get("hits");

                        for (JsonNode hit : scrollHits) {
                            updateMap(hit, numberOfHits, TFWQ);
                        }

                        scrollId = okapiBM25ScrollJSON.get("_scroll_id").asText();
                        totalHits = totalHits - 10000;
                    }
                }
            }

            SortedMap = sortMapByScore(okapiBM25Map);
            int rank = 0;

            for (Map.Entry m1 : SortedMap.entrySet()) {

                if (rank < 1000) {
                    rank = rank + 1;
                    writer.write(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " OkapiBM25\n");
                } else
                    break;
            }
            SortedMap.clear();
            okapiBM25Map.clear();
        }
        writer.close();
        restClient.close();
        reader.close();
    }

    private static void updateMap(JsonNode hit, long numberOfHits, int TFWQ) {

        String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
        int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

        String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
        int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

        String docNo = hit.get("_source").get("docNo").asText();

        Float OkapiBM25 = (float) (Math.log((DOCCount + 0.5) / (numberOfHits + 0.5)) * ((TFWD + (k1 * TFWD)) / (TFWD + k1 * ((1 - b) + b * (docLegth / docAverage)))) * ((TFWQ + (k2 * TFWQ)) / (TFWQ + k2)));
        okapiBM25Map.put(docNo,
                okapiBM25Map.get(docNo) == null ? OkapiBM25 : okapiBM25Map.get(docNo) + OkapiBM25);
    }
}
