package edu.neu.utility;


import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by paulomimahidharia on 7/3/17.
 */
@Getter
@Setter
public class ReturnData {

    Set<String> P;
    Set<String> S;

    HashMap<String, Set<String>> MP;
    HashMap<String, Set<String>> outlinkM;

    public ReturnData(HashMap<String, Set<String>> mp, HashMap<String, Set<String>> outlinkM, Set<String> p, Set<String> s) {
        P = p;
        S = s;
        this.MP = mp;
        this.outlinkM = outlinkM;
    }
}
