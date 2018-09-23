package edu.neu.utility;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Set;

/**
 * Created by paulomimahidharia on 7/6/17.
 */
@Setter
@Getter
public class RootNode {

    List<String> outLinks;
    List<String> inLinks;
}
