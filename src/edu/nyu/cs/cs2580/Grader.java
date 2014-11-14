package edu.nyu.cs.cs2580;

/**
 * Grading criteria.
 *
 * Grading will be done via the public APIs for the main classes:
 *   Indexer, Ranker, Document, Query, CorpusAnalyzer, LogMiner
 * Do NOT change the public APIs for those classes.
 *
 * In HW3, we will examine your implementation through the following tasks.
 *
 * NOTE: In addition to the normal score-based ranking of the results, you must
 * return: 1) PageRank for each result; 2) NumViews for each result.
 *
 *  a) PageRank verification: we will issue a set of queries to retrieve results
 *     using your search engine and examine the PageRanks of the results
 *     returned. 25 points.
 *
 *  b) NumViews verification: we will issue a set of queries to retrieve results
 *     using your search engine and examine the NumViews of the results
 *     returned. 10 points.
 *
 *  c) See writeup for grading on comparing PageRank and NumViews (15 points)
 *     and pseudo-relevance feedback (50 points).
 *
 * @author congyu
 */
public class Grader {
  Indexer _indexer;
  Ranker _ranker;

  public Grader() { }

  public void setIndexer(Indexer indexer) {
    _indexer = indexer;
  }

  public void setRanker(Ranker ranker) {
    _ranker = ranker;
  }

  public static void main(String[] args) {
  }
}
