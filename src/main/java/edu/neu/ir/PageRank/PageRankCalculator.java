package edu.neu.ir.PageRank;

import edu.neu.utility.ReturnData;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.Entry;

public class PageRankCalculator {

    public static int convCnt = 0;

    public static void main(String a[]) throws FileNotFoundException, UnsupportedEncodingException {



        double PrevPerp;
        double CurrPerp;

        //FOR MERGED INDEX
        //PrintWriter writer = new PrintWriter("PageRank.txt", "UTF-8");
        //ReturnData data = ExtractMergedInlinks.getData();

        //FOR WT2G
        PrintWriter writer = new PrintWriter("PageRankWT2G.txt", "UTF-8");
        ReturnData data = ExtractWT2G.getData();

        Set<String> P = data.getP();
        Set<String> S = data.getS();

        HashMap<String, Set<String>> M = data.getMP();
        HashMap<String, Set<String>> L = data.getOutlinkM();

        double n = ((double) P.size());
        System.out.println(n);
        System.out.println(M.size());
        System.out.println(L.size());
        System.out.println(S.size());

        HashMap<String, Double> PR = new HashMap<>();
        HashMap<String, Double> NewPR = new HashMap<>();

        double d = 0.85;
        int itr = 0;

        // Initialize to 1/Total #pages
        for (String p : P) {

            PR.put(p, ((double) 1 / n));
        }

        CurrPerp = Perplexity(PR);
        while (convCnt <= 4) {

            itr = itr + 1;
            System.out.println(itr + "   ITERATION  " + convCnt + "   " + CurrPerp);

            PrevPerp = CurrPerp;

            //Compute dangling mass
            Double sinkPR = (double) 0;
            for (String p : S) {
                sinkPR += PR.get(p);
            }

            // Update page rank
            for (String p : P) {
                NewPR.put(p, (1.0 - d) / n);
                Double spread = (d * sinkPR / n);

                //Add dangling mass
                NewPR.put(p, (NewPR.get(p) + spread));

                if (M.get(p) != null) {
                    for (String q : M.get(p)) {
                        Double val =  d * PR.get(q) / L.get(q).size();
                        NewPR.put(p, (double) NewPR.get(p) + val);
                    }
                }
            }

            for (String p : P) {
                PR.put(p, (NewPR.get(p)));
            }


            CurrPerp = Perplexity(PR);
            int i = (int) PrevPerp;
            int j = (int) CurrPerp;
            PerpCnt(i, j);
        }


        double sum = 0;
        for (Map.Entry m1 : PR.entrySet()) {
            sum = sum + (double) m1.getValue();
        }
        System.out.println("summation :" + sum);

        HashMap<String, Double> SortedMap = sortHM(PR);
        int count = 500;

        for (Map.Entry m1 : SortedMap.entrySet()) {

            if (count > 0) {

                writer.println(m1.getKey() + " " + m1.getValue());
            }
            count--;
        }

        writer.close();
    }

    private static double Perplexity(HashMap<String, Double> PR) {

        double entropy = 0;
        for (Entry<String, Double> m : PR.entrySet()) {
            entropy = (entropy + (m.getValue() * (Math.log(m.getValue()) / Math.log(2))));
            //	System.out.println(m.getValue());
        }
        //	System.out.println(perpSum);
        return Math.pow(2, -entropy);
    }

    private static void PerpCnt(int PrevPerp, int CurrPerp) {
        if (PrevPerp == CurrPerp) {
            convCnt = convCnt + 1;
        } else convCnt = 0;
    }

    private static HashMap<String, Double> sortHM(Map<String, Double> aMap) {

        Set<Entry<String, Double>> mapEntries = aMap.entrySet();
        List<Entry<String, Double>> aList = new LinkedList<Entry<String, Double>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<String, Double>>() {


            public int compare(Entry<String, Double> ele1,
                               Entry<String, Double> ele2) {

                return ele2.getValue().compareTo(ele1.getValue());
            }
        });

        Map<String, Double> aMap2 = new LinkedHashMap<String, Double>();
        for (Entry<String, Double> entry : aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }

        return (HashMap<String, Double>) aMap2;
    }
}
