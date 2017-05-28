package edu.neu.ir;

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
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.neu.utility.JsonProcessing.parseStringToJson;
import static edu.neu.utility.QueryProcessing.getStopWords;
import static edu.neu.utility.QueryProcessing.removeStopWordsFromQuery;
import static edu.neu.utility.SortMap.sortMapByScore;
import static edu.neu.utility.SortMap.sortMapByScoreDouble;

public class Extra {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String OUTPUT = "Laplace.txt";
    private final static String DATA_DIR = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection";
    private final static String DOC_PATTERN = "<DOC>\\s(.+?)</DOC>";
    private final static String DOCNO_PATTERN = "<DOCNO>(.+?)</DOCNO>";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";


    public static void main(String args[]) throws IOException, ParseException {

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
                continue;
            }
            JsonNode terms = termVectorResponseJson.get("term_vectors").get("text").get("terms");
            //System.out.println("TEMRS " + terms.toString());
            Iterator<Map.Entry<String, JsonNode>> nodeIterator =  terms.fields();

            while (nodeIterator.hasNext()) {

                Map.Entry<String, JsonNode> entry = nodeIterator.next();
                termTFBeanMap.put(entry.getKey(), new TermTFBean(docLength, entry.getValue().get("term_freq").asInt()));
            }

            docTermTFMap.put(docID, termTFBeanMap);
        }

        System.out.println(docTermTFMap.size());

        /*JsonObject vocabularyObj = Json.createObjectBuilder()
                .add("size", 0)
                .add("aggs", Json.createObjectBuilder()
                        .add("unique_terms", Json.createObjectBuilder()
                                .add("cardinality", Json.createObjectBuilder()
                                        .add("script", "doc['text'].values"))))
                .build();

        HttpEntity vocabularyEntity = new NStringEntity(vocabularyObj.toString(), ContentType.APPLICATION_JSON);
        Response vocabularyResponse = restClient.performRequest("GET", "/ap89_dataset/document/_search/", Collections.<String, String>emptyMap(), vocabularyEntity);
        ObjectNode vocabularyObject = parseStringToJson(EntityUtils.toString(vocabularyResponse.getEntity()));
        int vocabulary = vocabularyObject.get("aggregations").get("unique_terms").get("value").asInt();*/

        int vocabulary = 177992;
        System.out.println("VOCAB : " + vocabulary);

        // Read and store StopList terms
        Set<String> stopWords = getStopWords();

        File queryFile = new File(QUERY_FILE);
        BufferedReader br = new BufferedReader(new FileReader(queryFile));
        String query = null;
        while ((query = br.readLine()) != null) {

            HashMap<String, Double> LaplaceSmoothing = new HashMap<String, Double>();
            HashMap<String, Double> SortedMap = new HashMap<String, Double>();

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


                for(String key: docTermTFMap.keySet()){

                    //Get Doc Length

                    double LMTemp;
                    Map<String, TermTFBean> termTFMap = docTermTFMap.get(key);
                    if(termTFMap.size() <= 0) continue;

                    if(termTFMap.containsKey(word)){

                        System.out.println("TERM : "+word+" TF: "+termTFMap.get(word).getTf() + "DOC LENGTH :"+termTFMap.get(word).getDocLength() + "VOCAB : "+vocabulary);

                        Double term1 = (double) ((termTFMap.get(word).getTf()+1)/(termTFMap.get(word).getDocLength()+vocabulary));
                        LMTemp = Math.log(term1);
                    }else{

                        System.out.println("TERM : "+word + "DOC LENGTH :"+termTFMap.get(word).getDocLength() + "VOCAB : "+vocabulary);

                        Double term1 = (double) (1/(termTFMap.entrySet().iterator().next().getValue().getDocLength()+vocabulary));
                        LMTemp = Math.log(term1);
                    }

                    System.out.println("LMTemp" + LMTemp);

                    LaplaceSmoothing.put(key,
                             (LaplaceSmoothing.get(key) == null ? LMTemp : LaplaceSmoothing.get(key) + LMTemp));

                }
            }

            System.out.println("SIZE : " + LaplaceSmoothing.size());
            SortedMap = sortMapByScoreDouble(LaplaceSmoothing);
            System.out.println("SORTED SIZE : " + SortedMap.size());
            int rank = 0;

            for (Map.Entry m1 : SortedMap.entrySet()) {

                if (rank < 1000) {
                    rank = rank + 1;
                    writer.write(queryNo + " Q0 " + m1.getKey() + " " + rank + " " + m1.getValue() + " Laplace\n");
                } else
                    break;
            }
            SortedMap.clear();
            LaplaceSmoothing.clear();
        }
        writer.close();
        br.close();
    }
}