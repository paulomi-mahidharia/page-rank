package edu.neu.ir.HubsAuthority;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.neu.utility.RootNode;
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
import java.util.*;

import static edu.neu.utility.JsonProcessing.parseStringToJson;

public class GetRootSet {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";
    private final static int INLINE_THRESHOLD = 50;

    private static Map<String, RootNode> rootSet = new HashMap<>();
    //private static PrintWriter writer;


    public static void main(String args[]) throws IOException, ParseException {
        //writer = new PrintWriter("RootSet.txt", "UTF-8");
        Map<String, RootNode> localSet = getRootSet();
        System.out.println(localSet.size());
    }

    public static Map<String, RootNode> getRootSet() throws IOException, ParseException {

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        String query = "south korea ferry disaster";

        JsonObject esSearchObj = Json.createObjectBuilder()
                .add("size", 100)
                .add("query", Json.createObjectBuilder()
                        .add("match", Json.createObjectBuilder()
                                .add("text", query))).build();


        HttpEntity esSearchEntity = new NStringEntity(esSearchObj.toString(), ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest("GET", "bpp/document/_search/?scroll=1m", Collections.<String, String>emptyMap(), esSearchEntity);

        ObjectNode esSearchResponse = parseStringToJson(EntityUtils.toString(response.getEntity()));

        JsonNode hits = esSearchResponse.get("hits").get("hits");

        processHits(hits);

        String scrollId = esSearchResponse.get("_scroll_id").asText();

        while (rootSet.size() < 1000) {

            JsonObject scrollObject = Json.createObjectBuilder()
                    .add("scroll", "1m")
                    .add("scroll_id", scrollId)
                    .build();

            HttpEntity scrollEntity = new NStringEntity(scrollObject.toString(), ContentType.APPLICATION_JSON);
            Response scrollResponse = restClient.performRequest("POST", "/_search/scroll", Collections.<String, String>emptyMap(), scrollEntity);
            ObjectNode scrollJSON = parseStringToJson(EntityUtils.toString(scrollResponse.getEntity()));

            JsonNode scrollHits = scrollJSON.get("hits").get("hits");

            processHits(scrollHits);

            scrollId = scrollJSON.get("_scroll_id").asText();
        }

        //writer.close();
        return rootSet;
    }

    private static void processHits(JsonNode hits) {

        RootNode rootNode;

        for (JsonNode hit : hits) {

            rootNode = new RootNode();

            String docno = hit.get("_source").get("docno").asText();
            //writer.println(docno);
            List<String> outLinkList = getList(hit, "out_links");
            List<String> inlinkList = getList(hit, "in_links");

            int intSize = inlinkList.size() < INLINE_THRESHOLD ? inlinkList.size() : INLINE_THRESHOLD;

            int random = 0;
            if(intSize - INLINE_THRESHOLD > 50){
                int span = intSize - INLINE_THRESHOLD;
                Random rand = new Random();
                random = rand.nextInt(span);
            }

            rootNode.setInLinks(inlinkList.subList(random, random + intSize));
            rootNode.setOutLinks(outLinkList);

            rootSet.put(docno, rootNode);
        }
    }

    private static List<String> getList(JsonNode hit, String field) {

        try {
            if (hit.get("_source").get(field).toString().isEmpty())
                return new ArrayList<>();

            String rawField = hit.get("_source").get(field).toString();
            rawField = rawField.replace("[", "").replace("]", "").replace("\"", "");
            Set<String> fieldSet = new HashSet<>(Arrays.asList(rawField.split(",")));
            return new ArrayList<>(fieldSet);

        } catch (NullPointerException ne) {
            return new ArrayList<>();
        }
    }
}
