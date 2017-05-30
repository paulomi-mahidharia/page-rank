package edu.neu.beans;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class TermInfoBean {

    private int docLength;
    private int tf;
    private int ttf;

    private double maxScore;

    public TermInfoBean(int term, int tf) {
        this.docLength = term;
        this.tf = tf;
    }

    public TermInfoBean(int term, int tf, int ttf) {
        this.docLength = term;
        this.tf = tf;
        this.ttf = ttf;
    }

    public TermInfoBean(int term, int tf, int ttf, double maxScore) {
        this.docLength = term;
        this.tf = tf;
        this.ttf = ttf;
        this.maxScore = maxScore;

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

    public double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(double maxScore) {
        this.maxScore = maxScore;
    }

    public int getTtf() {
        return ttf;
    }

    public void setTtf(int ttf) {
        this.ttf = ttf;
    }
}
