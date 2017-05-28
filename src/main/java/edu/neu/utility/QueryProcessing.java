package edu.neu.utility;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by paulomimahidharia on 5/27/17.
 */
public class QueryProcessing {

    private final static String STOPLIST_FILE = "/Users/paulomimahidharia/Desktop/IR/resources/AP_DATA/stoplist.txt";

    public static StringBuffer removeStopWordsFromQuery(String query, Set<String> stopWords){

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

        return cleanQuery;
    }

    public static Set<String> getStopWords() throws IOException {

        File stopListFile = new File(STOPLIST_FILE);
        BufferedReader stopListFileBufferedReader = new BufferedReader(new FileReader(stopListFile));

        Set<String> stopWords = new HashSet<String>();

        String stopListTerm = null;
        while ((stopListTerm = stopListFileBufferedReader.readLine()) != null) {
            stopWords.add(stopListTerm.trim());
        }

        stopListFileBufferedReader.close();
        return stopWords;
    }
}
