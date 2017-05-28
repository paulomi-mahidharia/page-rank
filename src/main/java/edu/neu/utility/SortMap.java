package edu.neu.utility;

import java.util.*;

/**
 * Created by paulomimahidharia on 5/27/17.
 */
public class SortMap {

    public static HashMap<String, Float> sortMapByScore(Map<String, Float> map) {

        Set<Map.Entry<String, Float>> mapEntries = map.entrySet();
        List<Map.Entry<String, Float>> aList = new LinkedList<Map.Entry<String, Float>>(mapEntries);

        Collections.sort(aList, new Comparator<Map.Entry<String, Float>>() {


            public int compare(Map.Entry<String, Float> ele1,
                               Map.Entry<String, Float> ele2) {

                return ele2.getValue().compareTo(ele1.getValue());
            }
        });

        Map<String, Float> aMap2 = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }

        return (HashMap<String, Float>) aMap2;
    }

}
