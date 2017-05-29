package edu.neu.beans;

/**
 * Created by paulomimahidharia on 5/28/17.
 */
public class TermHitsInfo {


    int TF;
    int docLength;
    double maxScore;

    public TermHitsInfo(int TF, int docLength, double maxScore) {

        this.TF = TF;
        this.docLength = docLength;
        this.maxScore = maxScore;
    }


    public int getTF() {
        return TF;
    }

    public void setTF(int TF) {
        this.TF = TF;
    }

    public int getDocLength() {
        return docLength;
    }

    public void setDocLength(int docLength) {
        this.docLength = docLength;
    }

    public double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(double maxScore) {
        this.maxScore = maxScore;
    }
}
