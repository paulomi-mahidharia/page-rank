package edu.neu.ir.HubsAuthority;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.parser.ParseException;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static edu.neu.utility.JsonProcessing.parseStringToJson;

public class GetAllUrls {

    private final static String HOST = "localhost";
    private final static int PORT = 9200;
    private final static String SCHEME = "http";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, ParseException {

        RestClient restClient = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME),
                new HttpHost(HOST, PORT + 1, SCHEME)).build();

        PrintWriter writer = new PrintWriter("allURLs.txt", "UTF-8");
        HashSet<String> AllUrls = new HashSet<>();

        JsonObject TFObject = Json.createObjectBuilder()
                .add("from", 0)
                .add("size", 100)
                .add("_source", true)
                .build();

        HttpEntity TFEntity = new NStringEntity(TFObject.toString(), ContentType.APPLICATION_JSON);

        Response TFResponse = restClient.performRequest("GET", "bpp/_search/?scroll=1m", Collections.<String, String>emptyMap(), TFEntity);
        ObjectNode TFJSON = parseStringToJson(EntityUtils.toString(TFResponse.getEntity()));

        long numberOfHits = TFJSON.get("hits").get("total").asLong();

        JsonNode hits = TFJSON.get("hits").get("hits");

        int count = 1;

        for (JsonNode hit : hits) {

            System.out.println(count);

            AllUrls.add(hit.get("_source").get("docno").asText());
            writer.println(hit.get("_source").get("docno").asText());
            count++;
        }

        String scrollId = "";
        long totalHits = numberOfHits;

        if (totalHits > 100) {

            scrollId = TFJSON.get("_scroll_id").asText();
            while (totalHits > 100) {

                JsonObject okaiBM25ScrollObject = Json.createObjectBuilder()
                        .add("scroll", "1m")
                        .add("scroll_id", scrollId)
                        .build();

                HttpEntity okapiBM25ScrollEntity = new NStringEntity(okaiBM25ScrollObject.toString(), ContentType.APPLICATION_JSON);
                Response okapiBM25ScrollResponse = restClient.performRequest("POST", "/_search/scroll", Collections.<String, String>emptyMap(), okapiBM25ScrollEntity);
                ObjectNode okapiBM25ScrollJSON = parseStringToJson(EntityUtils.toString(okapiBM25ScrollResponse.getEntity()));

                JsonNode scrollHits = okapiBM25ScrollJSON.get("hits").get("hits");

                for (JsonNode hit : scrollHits) {

                    System.out.println(count);
                    AllUrls.add(hit.get("_source").get("docno").asText());
                    writer.println(hit.get("_source").get("docno").asText());

                    count++;
                }

                scrollId = okapiBM25ScrollJSON.get("_scroll_id").asText();
                totalHits = totalHits - 100;
            }


        }


        writer.close();
        System.out.println(AllUrls.size());

        /*Settings settings = Settings.builder()
                .put("client.transport.sniff", true)
                .put("cluster.name", "paulbiypri")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        PrintWriter writer = new PrintWriter("allURLs.txt", "UTF-8");
        HashSet<String> AllUrls = new HashSet<>();

        SearchResponse scrollResp = client.prepareSearch("bpp")
                .setScroll(new TimeValue(60000))
                .setTypes("document")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(0)
                .setSize(10000)
                .get();

        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                AllUrls.add(hit.getId());
                writer.println(hit.getId());
            }
            System.out.println("10000 done");
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();

            //Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        writer.close();
        System.out.println(AllUrls.size());*/
    }
}
