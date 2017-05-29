package edu.neu.beans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.neu.utility.TermData;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.search.Sort;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.*;
import java.util.*;

/**
 * Created by paulomimahidharia on 5/21/17.
 */
public class TFID {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";
    private final static String STOPLIST_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/stoplist.txt";
    private static Set<String> stopWords = new HashSet<String>();
    private static Map<String, List<TermData>> TFIDMap = new HashMap<String, List<TermData>>();

    public static void main(String args[]) throws IOException, ParseException {

        File queryFile = new File(QUERY_FILE);
        File stopListFile = new File(STOPLIST_FILE);
        BufferedReader queryFileBufferedReader = new BufferedReader(new FileReader(queryFile));
        BufferedReader stopListFileBufferedReader = new BufferedReader(new FileReader(stopListFile));

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        // Read and store StopList terms
        String stopListTerm = null;
        while ((stopListTerm = stopListFileBufferedReader.readLine()) != null) {
            stopWords.add(stopListTerm.trim());
        }
        System.out.println(stopWords.size());

        // Remove StopList terms from the query
        String query = null;
        while ((query = queryFileBufferedReader.readLine()) != null) {

            if (query.length() <= 3) {
                break;
            }

            String queryNo = query.substring(0, 5).replace(".", "").trim();
            query = query.substring(5).trim();

            StringBuffer cleanQuery = removeStopWords(query, stopWords);

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

                List<TermData> termDataList = new ArrayList<TermData>();

                JsonNode hits = TFJSON.get("hits").get("hits");
                TermData termData = null;

                for (JsonNode hit : hits) {

                    String TFWDRaw = hit.get("fields").get("index_tf").toString().replace("[", "").replace("]", "").trim();
                    int TFWD = TFWDRaw.equals("") ? 0 : Integer.parseInt(TFWDRaw);

                    String DocLengthRaw = hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim();
                    int docLegth = TFWDRaw.equals("") ? 0 : Integer.parseInt(DocLengthRaw);

                    String docNo = hit.get("_source").get("docNo").asText();

                    termData = new TermData(docNo, TFWD, docLegth, numberOfHits);
                    termDataList.add(termData);
                }

                if (TFIDMap.containsKey(word)) {
                    List<TermData> termDataListExisting = TFIDMap.get(word);
                    termDataListExisting.addAll(termDataList);
                    TFIDMap.put(word, termDataListExisting);
                } else
                    TFIDMap.put(word, termDataList);
            }
        }

        restClient.close();
        queryFileBufferedReader.close();
        stopListFileBufferedReader.close();

        calculateOkapi();
    }

    private static void calculateOkapi() throws IOException {

        File queryFile = new File(QUERY_FILE);
        BufferedReader queryFileBufferedReader = new BufferedReader(new FileReader(queryFile));
        double docAverage = (double) 20976545 / 84612;

        String query = null;
        while ((query = queryFileBufferedReader.readLine()) != null) {

            if (query.length() <= 3) {
                break;
            }

            String queryNo = query.substring(0, 5).replace(".", "").trim();
            query = query.substring(5).trim();

            StringBuffer cleanQuery = removeStopWords(query, stopWords);
            HashMap<String, Float> okapiTFMAP = new HashMap<String, Float>();
            HashMap<String, Float> SortedMap;

            //Get each word in the query
            String[] cleanQueryWords = cleanQuery.toString().trim().split(" ");
            for (String word : cleanQueryWords) {

                System.out.println("TFIDMAP SIZE :" +TFIDMap.get(word).size());

                for (TermData termData : TFIDMap.get(word)) {

                    String DocID = termData.getDocID();
                    int TFWD = termData.getTFWD();
                    long lenD = termData.getDocLength();

                    Float OkapiTFWD = (float) (TFWD / (TFWD + 0.5 + (1.5 * (lenD / docAverage))));
                    okapiTFMAP.put(DocID, okapiTFMAP.containsKey(DocID) ? (okapiTFMAP.get(DocID) + OkapiTFWD) : OkapiTFWD);
                }
            }

            System.out.println("Okapi SIZE : " + okapiTFMAP.size());
            SortedMap = sortHM(okapiTFMAP);
            System.out.println("SortedMap SIZE : " + SortedMap.size());
            PrintWriter writer = new PrintWriter("okapi_TF.txt", "UTF-8");
            int rank = 0;

            for(String key : SortedMap.keySet()){
                System.out.println(key + " : "+ SortedMap.get(key));
            }

            for (Map.Entry m1 : SortedMap.entrySet()) {

                while (rank < 1000) {
                    rank = rank + 1;
                    System.out.println(queryNo + "  Q0  " + m1.getKey() + "  " + rank + "  " + m1.getValue() + "  OkapiTF");
                }
            }

            SortedMap.clear();
            okapiTFMAP.clear();
        }

        System.out.println("DONE !");
        queryFileBufferedReader.close();
    }

    private static HashMap<String, Float> sortHM(HashMap<String, Float> okapiTFMAP) {

        Set<Map.Entry<String, Float>> mapEntries = okapiTFMAP.entrySet();
        List<Map.Entry<String, Float>> aList = new LinkedList<Map.Entry<String, Float>>(mapEntries);

        Collections.sort(aList, new Comparator<Map.Entry<String, Float>>() {


            public int compare(Map.Entry<String, Float> ele1,
                               Map.Entry<String, Float> ele2) {

                return ele2.getValue().compareTo(ele1.getValue());
            }
        });

        Map<String, Float> aMap2 = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }

        return (HashMap<String, Float>) aMap2;
    }

    private static StringBuffer removeStopWords(String query, Set<String> stopWords) {

        int index = 0;
        StringBuffer cleanQuery = new StringBuffer();

        while (index < query.length()) {

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

        return cleanQuery;
    }

    private static ObjectNode parseStringToJson(String s) throws ParseException, IOException {

        return new ObjectMapper().readValue(s, ObjectNode.class);
    }
}
