package edu.neu.ir;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.neu.utility.TermData;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by paulomimahidharia on 5/20/17.
 */
public class TF_IDF {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";
    private static long DOCCount = 0;
    private final static String STOPLIST_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/stoplist.txt";
    private final static String OUTPUT = "TF_IDF.txt";

    public static void main(String args[]) throws IOException, ParseException {

        File queryFile = new File(QUERY_FILE);
        File stopListFile = new File(STOPLIST_FILE);
        File okapiTFOutput = new File(OUTPUT);

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Response countResponse = restClient.performRequest("GET", "/ap_dataset/document/_count");
        ObjectNode countJson = parseStringToJson(EntityUtils.toString(countResponse.getEntity()));
        DOCCount = Long.parseLong(countJson.get("count").toString());
        System.out.println(DOCCount);

        double docAverage = (double) 20976545/84612;
        BufferedWriter writer = new BufferedWriter(new FileWriter(okapiTFOutput));

        BufferedReader stopListFileBufferedReader = new BufferedReader(new FileReader(stopListFile));

        // Read and store StopList terms
        Set<String> stopWords = new HashSet<String>();

        String stopListTerm = null;
        while ((stopListTerm = stopListFileBufferedReader.readLine()) != null) {
            stopWords.add(stopListTerm.trim());
        }
        System.out.println(stopWords.size());

        BufferedReader br = new BufferedReader(new FileReader(queryFile));
        String query = null;
        while ((query = br.readLine()) != null) {

            //for each query
            if (query.length() <= 3) {
                break;
            }

            HashMap<String, Float> okapiTFMAP = new HashMap<String, Float>();
            HashMap<String, Float> SortedMap = new HashMap<String, Float>();

            String queryNo = query.substring(0, 3).replace(".", "").trim();

            System.out.println("QUERY NO : "+queryNo);

            query = query.substring(5).trim();

            StringBuffer cleanQuery = new StringBuffer();
            int index = 0;

            while (index < query.length()) {

                // the only word delimiter supported is space, if you want other
                // delimiters you have to do a series of indexOf calls and see which
                // one gives the smallest index, or use regex
                int nextIndex = query.indexOf(" ", index);
                if (nextIndex == -1) {
                    nextIndex = query.length() - 1;
                }
                String word = query.substring(index, nextIndex);
                if (!stopWords.contains(word.toLowerCase())) {
                    cleanQuery.append(word);
                    if (nextIndex < query.length()) {
                        // this adds the word delimiter, e.g. the following space
                        cleanQuery.append(query.substring(nextIndex, nextIndex + 1));
                    }
                }
                index = nextIndex + 1;
            }

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

                long numberOfHits = TFJSON.get("hits").get("total").asLong();

                JsonNode hits = TFJSON.get("hits").get("hits");

                for (JsonNode hit : hits) {

                    //System.out.println(hit.toString());

                    String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
                    int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

                    String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
                    int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

                    String docNo = hit.get("_source").get("docNo").asText();

                    //	System.out.println( DocNo);

                    Float OkapiTFWD = (float) (TFWD / (TFWD + 0.5 + (1.5 * (docLegth / docAverage))));
                    Float TF_IDFWD = (float) (OkapiTFWD * (Math.log(DOCCount/numberOfHits)));
                    okapiTFMAP.put(docNo,
                            okapiTFMAP.get(docNo) == null ? TF_IDFWD : okapiTFMAP.get(docNo) + TF_IDFWD);

                }
            }

            System.out.println("SIZE : " + okapiTFMAP.size());
            SortedMap = sortHM(okapiTFMAP);
            System.out.println("SORTED SIZE : " + SortedMap.size());
            int rank = 0;

            for (Map.Entry m1 : SortedMap.entrySet()) {

                if (rank < 1000) {
                    rank = rank + 1;
                    writer.write(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " TF_IDF\n");
                    //System.out.println(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " OkapiTF");
                } else
                    break;
            }
            SortedMap.clear();
            okapiTFMAP.clear();
        }
        writer.close();
        restClient.close();
        br.close();
        stopListFileBufferedReader.close();
    }

    private static ObjectNode parseStringToJson(String s) throws ParseException, IOException {

        return new ObjectMapper().readValue(s, ObjectNode.class);
    }

    private static HashMap<String, Float> sortHM(Map<String, Float> aMap) {

        Set<Map.Entry<String,Float>> mapEntries = aMap.entrySet();
        List<Map.Entry<String,Float>> aList = new LinkedList<Map.Entry<String,Float>>(mapEntries);

        Collections.sort(aList, new Comparator<Map.Entry<String,Float>>() {


            public int compare(Map.Entry<String, Float> ele1,
                               Map.Entry<String, Float> ele2) {

                return ele2.getValue().compareTo(ele1.getValue());
            }
        });

        Map<String,Float> aMap2 = new LinkedHashMap<String, Float>();
        for(Map.Entry<String,Float> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }

        return (HashMap<String, Float>) aMap2;
    }
}
