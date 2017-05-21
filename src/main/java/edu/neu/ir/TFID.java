package edu.neu.ir;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by paulomimahidharia on 5/21/17.
 */
public class TFID {

    private final static String QUERY_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt";
    private final static String STOPLIST_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/stoplist.txt";


    public static void main(String args[]) throws IOException {

        File queryFile = new File(QUERY_FILE);
        File stopListFile = new File(STOPLIST_FILE);
        BufferedReader queryFileBufferedReader = new BufferedReader(new FileReader(queryFile));
        BufferedReader stopListFileBufferedReader = new BufferedReader(new FileReader(stopListFile));

        // Read and store StopList terms
        Set<String> stopWords = new HashSet<String>();

        String stopListTerm = null;
        while((stopListTerm = stopListFileBufferedReader.readLine()) != null){
            stopWords.add(stopListTerm.trim());
        }
        System.out.println(stopWords.size());

        // Remove StopList terms from the query
        String query = null;
        while((query = queryFileBufferedReader.readLine()) != null){

            if (query.length() <= 3)
            {break;}

            String queryNo = query.substring(0, 3).replace(".", "").trim();
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

            System.out.println(query);
            System.out.println("NEW : " + cleanQuery.toString());
        }


        /*Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));*/
    }


}
