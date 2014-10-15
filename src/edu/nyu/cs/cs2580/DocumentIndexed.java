package edu.nyu.cs.cs2580;

import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to incorporate any additional
 * information needed for your favorite ranker.
 */
public class DocumentIndexed extends Document {
  private static final long serialVersionUID = 9184892508124423115L;

   
//  private Vector<Integer> _titleTokens = new Vector<Integer>();
  private Set<Integer> _uniqueBodyTokens = new HashSet<Integer>();
  
  public DocumentIndexed(int docid) {
    super(docid);
  }
  
//  public void setTitleTokens(Vector<Integer> titleTokens) {
//    _titleTokens = titleTokens;
//  }

//  public Vector<Integer> getTitleTokens() {
//    return _titleTokens;
//  }

//  public Vector<String> getConvertedTitleTokens() {
//    return _indexer.getTermVector(_titleTokens);
//  }

  public void setUniqueBodyTokens(Set<Integer> bodyTokens) {
    _uniqueBodyTokens = bodyTokens;
  }

  public Set<Integer> getUniqueBodyTokens() {
   return _uniqueBodyTokens;
  }

//  public Vector<String> getConvertedBodyTokens() {
//    return _indexer.getTermVector(_bodyTokens);
//  } 
}
