package edu.neu.ir;


import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Extra {
    public static void main(String[] args) throws IOException {

        File IPfolder = new File("/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/ap89_collection");
        File[] listOfFiles = IPfolder.listFiles();
        Map<String, String> hmDocs = new HashMap<String, String>();

        PrintWriter writer = new PrintWriter("LSResults.txt", "UTF-8");
        Float Vocabulary = (float) 177992;


        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        BulkRequestBuilder br = client.prepareBulk();
        int count = 0;

        for (int i = 0; i < listOfFiles.length; i++) {
//begin creating hash map
            File mFile = new File(listOfFiles[i].getPath());
            String str = FileUtils.readFileToString(mFile);
            //  Extract DOC
            Pattern pattern = Pattern.compile("<DOC>\\s(.+?)</DOC>", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(str);

            while (matcher.find())

            {
                count++;
                // Save DOC
                String docTemp = matcher.group(1);

                // Extract DOCNO

                final Pattern pattern1 = Pattern.compile("<DOCNO>(.+?)</DOCNO>");
                final Matcher matcher1 = pattern1.matcher(docTemp);
                matcher1.find();
                String docNoTemp1 = matcher1.group(1);
                String docNoTemp = docNoTemp1.trim();
                // System.out.println(docNoTemp);

                // Extract TEXT


                Pattern pattern2 = Pattern.compile("<TEXT>\\s(.+?)</TEXT>", Pattern.DOTALL);
                Matcher matcher2 = pattern2.matcher(docTemp);

                String textTemp = "";

                while (matcher2.find()) {
                    // System.out.println("TEXT");
                    textTemp = textTemp.concat(matcher2.group(1));
                    // textTemp = matcher2.group(1);
                    // textTemp.concat(matcher2.group(1));
                }
                // System.out.println(textTemp);

                // Create Hash Entry
                textTemp = textTemp.replaceAll("\n", " ");

                hmDocs.put(docNoTemp, textTemp);

            }
//end creating hash map
        }

        File QueryFile = new File("/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/query_desc.51-100.short.txt");

        BufferedReader br1 = new BufferedReader(new FileReader(QueryFile));

        HashMap<String, Float> LaplaceSmoothing = new HashMap<String, Float>();
        HashMap<String, Float> SortedMap = new HashMap<String, Float>();
        String FullQueryTemp = "";
        while ((FullQueryTemp = br1.readLine()) != null) {
            if (FullQueryTemp.length() <= 3)
                break;


            String queryNo = FullQueryTemp.substring(0, 3);
            queryNo = queryNo.replace(".", "");
            queryNo = queryNo.trim();

            System.out.println("QUERY : "+queryNo);


            String[] queryparams = FullQueryTemp.substring(5, FullQueryTemp.length()).split("\\s+");
// 	 for(String WordQueryTemp: queryparams)
// 	 {
// 		 System.out.println(WordQueryTemp);
// 	 }


            for (Map.Entry m : hmDocs.entrySet())

            {
                Double LM = 0.0;
                String key = (String) m.getKey();

                for (String WordQueryTemp : queryparams) {
                    WordQueryTemp = WordQueryTemp.replace(".", " ");
                    WordQueryTemp = WordQueryTemp.trim();
                    // 	 System.out.println(WordQueryTemp);
                    // 	 System.out.println(key);

                    final Map<String, Object> params = new HashMap<String, Object>();
                    params.put("term", WordQueryTemp);
                    params.put("field", "text");

                    SearchResponse response = client.prepareSearch("ap_dataset")
                            .setTypes("document")
                            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                            .setQuery(QueryBuilders.termQuery("docno", key))
                            .setFrom(0)
                            .setSize(3)
                            .addScriptField("getDOCLENGTH", (new Script("doc['text'].values.size()")))
                            .addScriptField("getTF", (new Script(ScriptType.INLINE, "groovy", "_index['text']['govern'].tf()", params)))
                            .get();

                    //   System.out.println("res");

                    String DocID = "";
                    Float LMTemp = (float) 0;
                    for (SearchHit hit : response.getHits().hits()) {
                        DocID = hit.getId();
                        //	 System.out.println(DocID);
                        Integer TFWD = (hit.getFields().get("getTF").getValue());
                        Integer lenD = (hit.getFields().get("getDOCLENGTH").getValue());
                        //	System.out.println(TFWD);
                        if (TFWD > 0) {
                            Float term1 = (float) ((TFWD + 1) / (lenD + Vocabulary));

                            LMTemp = (float) Math.log(term1);
                            //  System.out.println(term1);

//                      LM= LM + LMTemp;
                            //  System.out.println(LM);

                        } else {
                            Float term1 = (float) ((0 + 1) / (lenD + Vocabulary));

                            LMTemp = (float) Math.log(term1);
                            // System.out.println(term1);


                            //  System.out.println(LM);
                        }


                        LM = LM + LMTemp;


                    }

                    LaplaceSmoothing.put(key,
                            (float) (LaplaceSmoothing.get(key) == null ? LM : LaplaceSmoothing.get(key) + LM));


                    //   System.out.println(LM);


                }

            }


            SortedMap = sortHM(LaplaceSmoothing);
            LaplaceSmoothing.clear();
            int rank = 0;

            for (Map.Entry sm : SortedMap.entrySet()) {

                if ((rank < 1000))

                {
                    rank = rank + 1;
                    writer.println(queryNo + "  Q0  " + sm.getKey() + "  " + rank + "  " + sm.getValue() + "  LAPLACE  ");
                } else break;

            }


            LaplaceSmoothing.clear();
            SortedMap.clear();
        }

        writer.close();
        System.out.println("DONE");
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
}


