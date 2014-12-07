package edu.nyu.cs.cs2580;

/**
 * Document with score.
 * 
 * @author fdiaz
 * @author congyu
 */
class ScoredDocument implements Comparable<ScoredDocument> {
  private Document _doc;
  private double _score;

  public ScoredDocument(Document doc, double score) {
    _doc = doc;
    _score = score;
  }

  public String asTextResult() {
    StringBuffer buf = new StringBuffer();
    buf.append(_doc._docid).append("\t");
    buf.append(_doc.getTitle()).append("\t");
    buf.append(_score).append("\t");
    buf.append("PageRank: ").append(_doc.getPageRank()).append("\t");
    buf.append("NumViews: ").append(_doc.getNumViews());
    return buf.toString();
  }

  /**
   * @CS2580: Student should implement {@code asHtmlResult} for final project.
   */
  public String asHtmlResult(String query) {
	StringBuffer buf = new StringBuffer();
    buf.append("<div style=\"line-height:5px\"><p><a style=\"text-decoration:none;cursor:pointer;color:#3300CC;font-size:18px\"");
    buf.append("href=\"./clicktrack?documentId=").append(_doc._docid);
    buf.append("&query=").append(query).append("\">");
    buf.append(_doc.getTitle());
    buf.append("</a></p>");
    buf.append("<p>Document ID: "+_doc._docid+"</p>");
    buf.append("PageRank: "+_doc.getPageRank()+" NumViews: "+_doc.getNumViews());
    buf.append("<p style=\"color:green;font-size:14px;\">");
    
    String[] urlParts = _doc.getUrl().split("/");
    if(urlParts.length > 5)
    	buf.append(urlParts[0] + "/" + urlParts[1] + "/" + urlParts[2] + "/.../" + urlParts[urlParts.length-2] + "/" + urlParts[urlParts.length-1]);
    else
    	buf.append(_doc.getUrl());
    
    buf.append("</p></div>");
    return buf.toString();
  }

  @Override
  public int compareTo(ScoredDocument o) {
    if (this._score == o._score) {
      return 0;
    }
    return (this._score > o._score) ? 1 : -1;
  }
  
  public int getDocId()
  {
	  return _doc._docid;
  }
  
  public String getDocTitle(){
  	return _doc.getTitle();
  }
  
  public double getScore(){
  	return _score;
  }
}
