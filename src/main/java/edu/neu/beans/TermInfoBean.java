package edu.neu.beans;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class TermInfoBean {

    private int docLength;
    private int tf;
    private int ttf;
    private int df;

    public TermInfoBean(int docLength, int tf) {
        this.docLength = docLength;
        this.tf = tf;
    }

    public TermInfoBean(int docLength, int tf, int ttf) {
        this.docLength = docLength;
        this.tf = tf;
        this.ttf = ttf;
    }

    public TermInfoBean(int docLength, int tf, int ttf, int df) {
        this.docLength = docLength;
        this.tf = tf;
        this.ttf = ttf;
        this.df = df;
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

    public int getTtf() {
        return ttf;
    }

    public void setTtf(int ttf) {
        this.ttf = ttf;
    }

    public int getDf() {
        return df;
    }

    public void setDf(int df) {
        this.df = df;
    }
}
