package edu.nyu.cs.cs2580;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

/**
 * The basic implementation of a Document.  Only the most basic information are
 * maintained in this class. Subclass should implement additional information
 * for display or ranking, such as snippet, term vectors, anchors, etc.
 *
 * In HW1: instructors provide {@link DocumentFull}.
 *
 * In HW2: students must implement the more efficient {@link DocumentIndexed}.
 *
 * In HW3: students must incorporate the PageRank and NumViews based on corpus
 * and log analyses.
 *
 * @author fdiaz
 * @author congyu
 */
class Document implements Serializable {
  private static final long serialVersionUID = -539495106357836976L;

  /**
   * A simple checker to see if a given document is present in our corpus.
   * This is provided for illustration only.
   */
  public static class HeuristicDocumentChecker {
    private static MessageDigest MD = null;

    private Set<BigInteger> _docsInCorpus = null;

    public HeuristicDocumentChecker() throws NoSuchAlgorithmException {
      if (MD == null) {
        MD = MessageDigest.getInstance("MD5");
      }
      _docsInCorpus = new HashSet<BigInteger>();
    }

    public void addDoc(String name) {
      if (MD != null) {
        _docsInCorpus.add(new BigInteger(MD.digest(name.getBytes())));
      }
    }

    public int getNumDocs() {
      return _docsInCorpus.size();
    }

    public boolean checkDoc(String name) {
      if (MD == null) {
        return false;
      }
      return _docsInCorpus.contains(new BigInteger(MD.digest(name.getBytes())));
    }
  }

  public int _docid;

  // Basic information for display
  private String _title = "";
  private String _url = "";

  // Basic information for ranking
  private float _pageRank = 0.0f;
  private int _numViews = 0;

  public Document(int docid) {
    _docid = docid;
  }

  public String getTitle() {
    return _title;
  }

  public void setTitle(String title) {
    this._title = title;
  }

  public String getUrl() {
    return _url;
  }

  public void setUrl(String url) {
    this._url = url;
  }

  public float getPageRank() {
    return _pageRank;
  }

  public void setPageRank(float pageRank) {
    this._pageRank = pageRank;
  }

  public int getNumViews() {
    return _numViews;
  }

  public void setNumViews(int numViews) {
    this._numViews = numViews;
  }
}
