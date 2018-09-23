package edu.neu.ir.HubsAuthority;

import edu.neu.utility.RootNode;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.*;

import static edu.neu.ir.HubsAuthority.GetRootSet.getRootSet;

public class HandA2 {

    public static void main(String a[]) throws IOException, ParseException {

        Settings settings = Settings.builder().put("client.transport.sniff", true)
                .put("cluster.name", "paulbiypri").build();

        Client client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        int con = 0;
        boolean converge;

        HashMap<String, Double> Hub = new HashMap<>();
        HashMap<String, Double> Authority = new HashMap<>();

        PrintWriter writerH = new PrintWriter("Hubs.txt", "UTF-8");
        PrintWriter writerA = new PrintWriter("Authority.txt", "UTF-8");

        Map<String, RootNode> rootSet = getRootSet();
        for (String docno : rootSet.keySet()) {

            Hub.put(docno, 1.0);
            Authority.put(docno, 1.0);
        }

        //Initialization
        while (con < 31) {

            HashMap<String, Double> oauthmap = Authority;
            HashMap<String, Double> ohubmap = Hub;

            // get out links and add its authority score
            for (String key : Hub.keySet()) {

                double hub = 0;
                RootNode rootNode = rootSet.get(key);
                List<String> outlinks = rootNode.getOutLinks();

                for (String outlink : outlinks) {

                    if (oauthmap.get(outlink) != null) {
                        //System.out.println("updating hub");
                        hub = hub + oauthmap.get(outlink);
                    }
                }

                if (hub != 0) {
                    Hub.put(key, hub);
                }
            }

            // get in links and add its hub score
            for (String key : Authority.keySet()) {

                double auth = 0;
                RootNode rootNode = rootSet.get(key);
                List<String> inlinks = rootNode.getInLinks();

                for (String inlink : inlinks) {

                    if (ohubmap.get(inlink) != null) {
                        auth = auth + ohubmap.get(inlink);
                    }
                }

                if (auth != 0) {
                    Authority.put(key, auth);
                }
            }

            //System.out.println("Authority Updated");

            //normalize authority
            double nauth = 0;
            for(String authority: Authority.keySet()){
                nauth = nauth + Math.pow(Authority.get(authority), 2);
            }

            nauth = Math.sqrt(nauth);

            for(String authority: Authority.keySet()){
                Authority.put(authority, Authority.get(authority)/ nauth);
            }

            //normalize Hub
            double nhub = 0;
            for(String hub: Hub.keySet()){
                nhub =  nhub + Math.pow(Hub.get(hub), 2) ;
            }
            nhub = Math.sqrt(nhub);

            for(String hub: Hub.keySet()){
                Hub.put(hub, Hub.get(hub)/ nhub);
            }

            if(convergeCheck(oauthmap, Hub, ohubmap, Authority)){
                con = con + 1;
                converge = true;
                //System.out.println(con+": In converge");
            }
            else {con = 0;
                converge = false;
                //System.out.println(con+": NOT In converge");
                }

            System.out.println("nauth:"+nauth+"  nhub:"+nhub+"  Convergence:"+converge);
        }

        System.out.println("printing to files");

        HashMap<String, Double> SortedMapH = sortHM(Hub);
        int rank1 = 0;

        for(Map.Entry m1:SortedMapH.entrySet())
        {
            if(rank1 < 500){
                rank1 = rank1 + 1;
                writerH.println(m1.getKey()+"\t"+m1.getValue());
            }
            else
                break;
        }

        HashMap<String, Double> SortedMapA = sortHM(Authority);
        int rank = 0;

        for(Map.Entry m2:SortedMapA.entrySet())
        {
            if(rank < 500){
                rank = rank + 1;
                writerA.println(m2.getKey()+"\t"+m2.getValue());
            }
            else
                break;
        }

        writerA.close();
        writerH.close();
    }

    private static boolean convergeCheck(HashMap<String, Double> oauth, HashMap<String, Double> nauth,
                                         HashMap<String, Double> ohub, HashMap<String, Double> nhub) {

        double authscore = 0;
        double hubscore = 0;
        double authc = 0;
        double hubc = 0;

        for(String docno: oauth.keySet()){
            double diff = oauth.get(docno) - nauth.get(docno);
            //diff = Math.abs(diff);
            authc = authc + 1;
            authscore = authscore + diff;
        }

        for(String docno: ohub.keySet()){
            double diff = ohub.get(docno) - nhub.get(docno);
            //diff = Math.abs(diff);
            hubc = hubc + 1;
            hubscore = hubscore + diff;
        }

        double h =  Math.abs(hubscore/ hubc);
        double a =  Math.abs(authscore/ authc);

        double r = Math.abs(h - a);
        System.out.printf("hubbscore:%.15f authscore:%.15f DIFF=%.15f%n",h,a, Math.abs(h - a));
        System.out.println("hub"+h+"auth"+a);

        return Math.abs(h - a) <= 0.000000000000001;
    }

    private static HashMap<String, Double> sortHM(Map<String, Double> aMap) {

        Set<Map.Entry<String,Double>> mapEntries = aMap.entrySet();
        List<Map.Entry<String,Double>> aList = new LinkedList<Map.Entry<String,Double>>(mapEntries);

        Collections.sort(aList, new Comparator<Map.Entry<String,Double>>() {


            public int compare(Map.Entry<String, Double> ele1,
                               Map.Entry<String, Double> ele2) {

                return ele2.getValue().compareTo(ele1.getValue());
            }
        });

        Map<String,Double> aMap2 = new LinkedHashMap<String, Double>();
        for(Map.Entry<String,Double> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }

        return (HashMap<String, Double>) aMap2;
    }

}
