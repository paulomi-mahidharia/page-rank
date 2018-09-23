package edu.neu.ir.HubsAuthority;

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
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.neu.utility.JsonProcessing.parseStringToJson;

public class BM25 {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";

    private final static float b = (float) 0.75;
    private final static float k1 = (float) 1.2;
    private final static float k2 = (float) 100;

    private static double docAverage;
    private static long DOCCount;

    private static HashMap<String, Float> okapiBM25Map = null;

    public static void main(String[] args) throws IOException, ParseException {

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Response countResponse = restClient.performRequest("GET", "/bpp/document/_count");
        ObjectNode countJson = parseStringToJson(EntityUtils.toString(countResponse.getEntity()));
        DOCCount = Long.parseLong(countJson.get("count").toString());
        System.out.println(DOCCount);

        docAverage = (double) 68807287 / DOCCount;

        PrintWriter writer = new PrintWriter("OkapiBM25.txt", "UTF-8");

        // for each word in query
        okapiBM25Map = new HashMap<>();

        HashMap<String, Float> SortedMap;
        String query = "South Korea ferry disaster";

        String words[] = query.split(" ");
        for (String word : words) {

            word = word.toLowerCase().trim();

            int TFWQ = 0;
            Pattern p = Pattern.compile(word);
            Matcher m = p.matcher(query.toLowerCase());
            while (m.find()) {
                TFWQ++;
            }

            // Get stem for current word
            JsonObject stemObj = Json.createObjectBuilder()
                    .add("analyzer", "my_english")
                    .add("text", word)
                    .build();

            HttpEntity stemEntity = new NStringEntity(stemObj.toString(), ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest("GET", "bpp/_analyze", Collections.<String, String>emptyMap(), stemEntity);

            ObjectNode stemResponse = parseStringToJson(EntityUtils.toString(response.getEntity()));
            String stem = "";

            for (JsonNode tokenObj : stemResponse.get("tokens")) {
                stem = tokenObj.get("token").asText().trim();
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

            Response TFResponse = restClient.performRequest("GET", "/bpp/_search/?scroll=1m", Collections.<String, String>emptyMap(), TFEntity);
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
                    Response okapiBM25ScrollResponse = restClient.performRequest("POST", "/bpp/_search/scroll", Collections.<String, String>emptyMap(), okapiBM25ScrollEntity);
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

        // Sort by document rank
        SortedMap = sortHM(okapiBM25Map);
        //op sorted map
        // int count = 0;
        int rank = 0;

        for (Entry m1 : SortedMap.entrySet()) {

            if (rank < 1000) {
                rank = rank + 1;

                writer.println(m1.getKey());
                System.out.println(m1.getKey() + "\t" + m1.getValue());


            } else

                break;

        }
        SortedMap.clear();
        okapiBM25Map.clear();

        writer.close();
        //end for each query
    }


    private static HashMap<String, Float> sortHM(Map<String, Float> aMap) {

        Set<Entry<String, Float>> mapEntries = aMap.entrySet();
        List<Entry<String, Float>> aList = new LinkedList<Entry<String, Float>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<String, Float>>() {


            public int compare(Entry<String, Float> ele1,
                               Entry<String, Float> ele2) {

                return ele2.getValue().compareTo(ele1.getValue());
            }
        });

        Map<String, Float> aMap2 = new LinkedHashMap<String, Float>();
        for (Entry<String, Float> entry : aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }

        return (HashMap<String, Float>) aMap2;
    }

    private static void updateMap(JsonNode hit, long numberOfHits, int TFWQ) {

        String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
        int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

        String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
        int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

        String docNo = hit.get("_source").get("docno").asText();

        double term1 = Math.log((DOCCount + 0.5) / (numberOfHits + 0.5));
        double term2 = ((TFWD + (k1 * TFWD)) / (TFWD + k1 * ((1 - b) + b * (docLegth / docAverage))));
        double term3 = ((TFWQ + (k2 * TFWQ)) / (TFWQ + k2));

        Float OkapiBM25 = (float) (term1 * term2 * term3);
        okapiBM25Map.put(docNo, okapiBM25Map.get(docNo) == null ? OkapiBM25 : okapiBM25Map.get(docNo) + OkapiBM25);
    }
}