package edu.neu.ir;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
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

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by paulomimahidharia on 5/20/17.
 */
public class okapiTF {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";
    private static long DOCCount = 0;
    private static long DOCLength = 41030489;

    public static void main(String args[]) throws IOException, ParseException {

        File queryFile = new File(QUERY_FILE);

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        Response countResponse = restClient.performRequest("GET", "/ap_dataset/document/_count");
        ObjectNode countJson = parseStringToJson(EntityUtils.toString(countResponse.getEntity()));
        DOCCount = Long.parseLong(countJson.get("count").toString());
        System.out.println(DOCCount);

        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));

        /*Response searchResponse = restClient.performRequest("GET", "/ap_dataset/document/_search");
        //ObjectNode searchJson = parseStringToJson(EntityUtils.toString(searchResponse.getEntity()));
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(EntityUtils.toString(searchResponse.getEntity()));
        JsonNode hits = root.path("hits").path("hits");
        System.out.println(hits.size());

        long count = 0;
        if (hits.isArray()) {
            for (final JsonNode hit : hits) {
                count = count + hit.path("_source").path("text").asText().trim().split(" ").length;

            }
        }

        System.out.println(count);*/

        double docAverage = (double) DOCLength/DOCCount;
        PrintWriter writer = new PrintWriter("okapi_TF.txt", "UTF-8");
        BufferedReader br = new BufferedReader(new FileReader(queryFile));

        String query = null;
        while ((query = br.readLine()) != null){

            //for each query
            if (query.length() <= 3)
            {break;}

            HashMap<String, Float> okapiTFMAP = new HashMap<String, Float>();
            String queryNo = query.substring(0, 3).replace(".", "").trim();
            //System.out.println(queryNo);

            double OkapiTFD = 0.0;

            // for each word in query
            for (int i = 0+5; i <= query.length() - 1; i++){

                if (query.substring(i).startsWith(" ") || i == 0){

                    for (int j = i + 1 ; j <= query.length() - 1 ; j++){

                        if (query.substring(j).startsWith(" ") || j == query.length() - 1){

                            String word = query.substring(i, j).replace(".", "").trim();
                            System.out.println(word);

                            final Map<String, Object> params = new HashMap<String, Object>();
                            params.put("term", word);
                            params.put("field", "text");

                            //restClient.

                            SearchResponse searchResponse = client.prepareSearch("ap_dataset")
                                    .setTypes("document")
                                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                                    .setQuery(QueryBuilders.functionScoreQuery
                                            (QueryBuilders.termQuery("text", word),
                                                    new ScriptScoreFunctionBuilder(new Script( ScriptType.INLINE,"getTF", "groovy", params)))

                                            .boostMode(CombineFunction.REPLACE))
                                    .setFrom(0)
                                    .setSize(10000)
                                    .setExplain(true)
                                    .addScriptField("getDOCLENGTH", (new Script("doc['text'].values.size()")))
                                    .get();

                            SearchHits hits = searchResponse.getHits();

                            System.out.println("HITSS : "+hits.totalHits);
                            for (SearchHit hit:hits) {

                                String DocID = hit.getId();
                                Float TFWD = hit.getScore();
                                Integer lenD = (hit.getFields().get("getDOCLENGTH").getValue());
                            }
                        }
                    }
                }
            }
        }

        restClient.close();
    }

    private static ObjectNode parseStringToJson(String s) throws ParseException, IOException {

        return new ObjectMapper().readValue(s, ObjectNode.class);
    }
}
