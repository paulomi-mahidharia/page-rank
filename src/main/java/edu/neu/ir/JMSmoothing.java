package edu.neu.ir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
public class JMSmoothing {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String OUTPUT = "JMSmoothing.txt";
    private final static String DATA_DIR = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection";
    private final static String DOC_PATTERN = "<DOC>\\s(.+?)</DOC>";
    private final static String DOCNO_PATTERN = "<DOCNO>(.+?)</DOCNO>";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";

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
        int docCount = docNames.size();

        long sum_doc_freq = 13944841;
        for(String docID : docNames){

            //System.out.println("DOC : "+docID);
            Map<String, TermTFBean> termTFBeanMap = new HashMap<String, TermTFBean>();

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

            for(JsonNode hit: hits){
                docLength = Integer.parseInt(hit.get("fields").get("doc_length").toString().replace("[", "").replace("]", "").trim());
            }

            //Get doc TFs
            Response termVectorResponse = restClient.performRequest("GET", "/ap89_dataset/document/"+docID+"/_termvector");
            ObjectNode termVectorResponseJson = parseStringToJson(EntityUtils.toString(termVectorResponse.getEntity()));


            JsonNode docTermVector = termVectorResponseJson.get("term_vectors");
            if(docTermVector.toString().equals("{}")) {
                docTermTFMap.put(docID, termTFBeanMap);
                //System.out.println("DOC DOC : "+docID);
                continue;
            }

            JsonNode terms = termVectorResponseJson.get("term_vectors").get("text").get("terms");
            Iterator<Map.Entry<String, JsonNode>> nodeIterator =  terms.fields();

            while (nodeIterator.hasNext()) {

                Map.Entry<String, JsonNode> entry = nodeIterator.next();
                termTFBeanMap.put(entry.getKey(), new TermTFBean(docLength, entry.getValue().get("term_freq").asInt()));
            }

            docTermTFMap.put(docID, termTFBeanMap);
        }

        System.out.println(docTermTFMap.size());

        // Read and store StopList terms
        Set<String> stopWords = getStopWords();

        File queryFile = new File(QUERY_FILE);
        BufferedReader br = new BufferedReader(new FileReader(queryFile));
        String query = null;
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


                //Get max_score
                JsonObject maxScoreObject = Json.createObjectBuilder()
                        .add("size", 1)
                        .add("_source", true)
                        .add("query", Json.createObjectBuilder()
                                .add("match", Json.createObjectBuilder()
                                        .add("text", word)))
                        .build();
                HttpEntity maxScoreEntity = new NStringEntity(maxScoreObject.toString(), ContentType.APPLICATION_JSON);
                Response maxScoreResponse = restClient.performRequest("GET", "/ap89_dataset/_search/", Collections.<String, String>emptyMap(), maxScoreEntity);
                ObjectNode maxScoreNode = parseStringToJson(EntityUtils.toString(maxScoreResponse.getEntity()));


                long maxScore = maxScoreNode.get("hits").get("max_score").asLong();

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

                String term = (stem.equals("")) ? word : stem;

                for(String key: docTermTFMap.keySet()){

                    //Get Doc Length

                    double LMTemp;
                    Map<String, TermTFBean> termTFMap = docTermTFMap.get(key);
                    if(termTFMap.size() <= 0) {

                        double term1 = ( 0 + (0.4 * (maxScore) / (sum_doc_freq)));
                        double JM =  (Math.log(term1));

                        JMSmoothingMap.put(key,
                                (JMSmoothingMap.get(key) == null ? JM : JMSmoothingMap.get(key) + JM));
                        continue;
                    }

                    Double docLength = (double) termTFMap.entrySet().iterator().next().getValue().getDocLength();
                    Double TFWD;
                    if(termTFMap.containsKey(term)){

                        TFWD = (double) termTFMap.get(term).getTf()+1;

                    }else{
                        TFWD = (double) 0;

                    }

                    double term1 = ((0.6 * TFWD / docLength) + (0.4 * (maxScore - TFWD) / (sum_doc_freq - docLength)));
                    double JM =  (Math.log(term1));

                    JMSmoothingMap.put(key,
                            (JMSmoothingMap.get(key) == null ? JM : JMSmoothingMap.get(key) + JM));

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
}
