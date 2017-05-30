package edu.neu.ir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.neu.beans.TermInfoBean;
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

import static edu.neu.utility.FileParser.getDocIds;
import static edu.neu.utility.JsonProcessing.parseStringToJson;
import static edu.neu.utility.QueryProcessing.getStopWords;
import static edu.neu.utility.QueryProcessing.removeStopWordsFromQuery;
import static edu.neu.utility.SortMap.sortMapByScoreDouble;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class JMSmoothing {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";

    private final static String OUTPUT = "JMSmoothing.txt";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";

    public static void main(String args[]) throws IOException, ParseException {

        int vocabulary = 177992;

        File queryFile = new File(QUERY_FILE);
        BufferedReader reader = new BufferedReader(new FileReader(queryFile));

        File output = new File(OUTPUT);
        BufferedWriter writer = new BufferedWriter(new FileWriter(output));

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        /**
         * Get all Document ids
         */
        List<String> docNames = getDocIds();

        Map<String, Map<String, TermInfoBean>> docTermTFMap = new HashMap<String, Map<String, TermInfoBean>>();

        for (String docID : docNames) {

            Map<String, TermInfoBean> termTFBeanMap = new HashMap<String, TermInfoBean>();

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
            int docLength = 0;

            for (JsonNode hit : hits) {
                docLength = Integer.parseInt(hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim());
            }

            //Get doc TFs and TTFs
            JsonObject termStats = Json.createObjectBuilder()
                    .add("term_statistics", true)
                    .build();
            HttpEntity termStatsEntity = new NStringEntity(termStats.toString(), ContentType.APPLICATION_JSON);

            Response termVectorResponse = restClient.performRequest("GET", "/ap89_dataset/document/" + docID + "/_termvector", Collections.<String, String>emptyMap(), termStatsEntity);
            ObjectNode termVectorResponseJson = parseStringToJson(EntityUtils.toString(termVectorResponse.getEntity()));

            JsonNode docTermVector = termVectorResponseJson.get("term_vectors");

            // Ignore documents with NO terms
            if (docTermVector.toString().equals("{}")) {
                continue;
            }

            // Store term_vectors
            JsonNode terms = termVectorResponseJson.get("term_vectors").get("text").get("terms");
            Iterator<Map.Entry<String, JsonNode>> nodeIterator = terms.fields();

            while (nodeIterator.hasNext()) {

                Map.Entry<String, JsonNode> entry = nodeIterator.next();
                termTFBeanMap.put(entry.getKey(), new TermInfoBean(docLength, entry.getValue().get("term_freq").asInt(), entry.getValue().get("ttf").asInt()));
            }

            docTermTFMap.put(docID, termTFBeanMap);
        }

        System.out.println(docTermTFMap.size());

        // Read and store StopList terms
        Set<String> stopWords = getStopWords();

        // For each query
        String query;
        while ((query = reader.readLine()) != null) {

            HashMap<String, Double> JMSmoothingMap = new HashMap<String, Double>();
            HashMap<String, Double> SortedMap;

            //for each query
            if (query.length() <= 3) {
                break;
            }

            String queryNo = query.substring(0, 5).replace(".", "").trim();
            System.out.println("QUERY NO : " + queryNo);

            query = query.substring(5).trim();

            StringBuffer cleanQuery = removeStopWordsFromQuery(query, stopWords);
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
                    stem = tokenObj.get("token").asText();
                }

                if (stem.equals("")) continue; //Word is not important

                for (String key : docTermTFMap.keySet()) {

                    Map<String, TermInfoBean> termTFMap = docTermTFMap.get(key);

                    Double docLength = (double) termTFMap.entrySet().iterator().next().getValue().getDocLength();
                    Double TFWD = (double) 0;
                    Double TTF = 0.0001;

                    if (termTFMap.containsKey(stem)) {
                        TFWD = (double) termTFMap.get(stem).getTf();
                        TTF = (double) termTFMap.get(stem).getTtf();
                    }

                    double term1 = ((0.9 * TFWD / docLength) + (0.1 * TTF / vocabulary));
                    double JM = (Math.log(term1));

                    JMSmoothingMap.put(key, (JMSmoothingMap.get(key) == null ? JM : JMSmoothingMap.get(key) + JM));
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
        reader.close();
        restClient.close();

        System.out.println("DONE");

    }
}
