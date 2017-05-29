package edu.neu.beans;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class TermTFBean {

    private int docLength;
    private int tf;
    private double maxScore;

    public TermTFBean(int term, int tf) {
        this.docLength = term;
        this.tf = tf;

    }

    public TermTFBean(int term, int tf, double maxScore) {
        this.docLength = term;
        this.tf = tf;
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
}
