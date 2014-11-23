package edu.nyu.cs.cs2580;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
  private static final long serialVersionUID = 9184892508124423115L;

  private int _documentSize;
  private double _tfidfSumSquared;

  public void setDocumentSize(int size){
  	_documentSize = size;
  }
  
  public int getDocumentSize(){
  	return _documentSize;
  }

  public void setTfidfSumSquared(double tfidfSumSquared) {
    _tfidfSumSquared = tfidfSumSquared;
  }

  public double getTfidfSumSquared() {
    return _tfidfSumSquared;
  }

  public DocumentIndexed(int docid) {
    super(docid);
  }

}
