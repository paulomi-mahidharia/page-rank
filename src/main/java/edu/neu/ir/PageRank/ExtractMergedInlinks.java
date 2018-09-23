package edu.neu.ir.PageRank;

import edu.neu.utility.ReturnData;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

public class ExtractMergedInlinks {

    public static ReturnData getData() throws FileNotFoundException, UnsupportedEncodingException {

        PrintWriter writer = new PrintWriter("OutCnt.txt", "UTF-8");

        Set<String> P = new HashSet<>();
        Set<String> S = new HashSet<>();

        HashMap<String, Set<String>> MP = new HashMap<>();
        HashMap<String, Set<String>> outlinkM = new HashMap<>();

        HashMap<String, Double> LQ = new HashMap<>();

        //	 File f1 = new File("/Users/paulomimahidharia/Desktop/IR/PageRank/MergedGraph/temp.txt");
        //	 File f1 = new File("/Users/paulomimahidharia/Desktop/IR/PageRank/MergedGraph/wt2g_inlinks-2.txt");
        File f1 = new File("/Users/paulomimahidharia/Desktop/IR/PageRank/MergedGraph/mergedInlinks.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f1)));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                String[] links = line.split("=");
                //System.out.println(links.length);

                String key = links[0];
                P.add(key);
                //	System.out.println(Key);
                String[] rawLinks;
                Set<String> inlinks;

                try {
                    rawLinks = links[1].trim().split(" ");
                    inlinks = new HashSet<>(Arrays.asList(rawLinks));
                }catch (ArrayIndexOutOfBoundsException e){
                    inlinks = new HashSet<>();
                }

                //	if(links.length > 1){
                for (String inlink : inlinks) {
                    //System.out.println(links[k]);
                    Set<String> ol;
                    P.add(inlink);

                    if (outlinkM.get(inlink) == null) {
                        ol = new HashSet<String>();
                        ol.add(key);
                    } else {
                        ol = outlinkM.get(inlink);
                        ol.add(key);
                    }
                    outlinkM.put(inlink, ol);
                    LQ.put(inlink, LQ.get(inlink) == null ? 1 : LQ.get(inlink) + 1);
                }
                MP.put(key, inlinks);
            }
        } catch (IOException e) {

            e.printStackTrace();
        }

        S.addAll(P);
        for (Entry<String, Double> m : LQ.entrySet()) {

            String s = m.getKey();
            S.remove(s);
        }

        for (Entry<String, Double> m : LQ.entrySet()) {

            writer.println(m.getKey() + ":" + m.getValue());
        }
        return new ReturnData(MP, outlinkM, P, S);
    }
}
