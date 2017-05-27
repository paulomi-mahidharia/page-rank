package edu.neu.utility;

/**
 * Created by paulomimahidharia on 5/25/17.
 */
public class TermData {

    private String docID;
    private int TFWD;
    private int docLength;
    private long totalHits;

    public TermData(String docID, int TFWD, int docLength, long totalHits) {
        this.docID = docID;
        this.TFWD = TFWD;
        this.docLength = docLength;
        this.totalHits = totalHits;
    }

    public String getDocID() {
        return docID;
    }

    public void setDocID(String docID) {
        this.docID = docID;
    }

    public int getTFWD() {

        return TFWD;

    }

    public void setTFWD(int TFWD) {
        this.TFWD = TFWD;
    }

    public long getDocLength() {
        return docLength;
    }

    public void setDocLength(int docLength) {
        this.docLength = docLength;
    }

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
    }
}
