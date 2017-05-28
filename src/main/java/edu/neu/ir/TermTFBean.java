package edu.neu.ir;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class TermTFBean {

    private int docLength;
    private int tf;

    public TermTFBean(int term, int tf) {
        this.docLength = term;
        this.tf = tf;
    }

    public int getDocLength() {

        return docLength;
    }

    public void setDocLength(int docLength) {
        this.docLength = docLength;
    }

    public int getTf() {
        return tf;
    }

    public void setTf(int tf) {
        this.tf = tf;
    }
}
