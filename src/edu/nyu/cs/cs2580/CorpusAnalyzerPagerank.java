package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import static com.google.common.base.Preconditions.*;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {

  private static final double DAMPENING_FACTOR = 0.1;

  protected BiMap<String, Integer> _documents;
  protected List<Set<Integer>> adjacencyList;

  public CorpusAnalyzerPagerank(Options options) {
    super(options);
  }

  /**
   * This function processes the corpus as specified inside {@link _options}
   * and extracts the "internal" graph structure from the pages inside the
   * corpus. Internal means we only store links between two pages that are both
   * inside the corpus.
   * 
   * Note that you will not be implementing a real crawler. Instead, the corpus
   * you are processing can be simply read from the disk. All you need to do is
   * reading the files one by one, parsing them, extracting the links for them,
   * and computing the graph composed of all and only links that connect two
   * pages that are both in the corpus.
   * 
   * Note that you will need to design the data structure for storing the
   * resulting graph, which will be used by the {@link compute} function. Since
   * the graph may be large, it may be necessary to store partial graphs to
   * disk before producing the final graph.
   *
   * @throws IOException
   */
  @Override
  public void prepare() throws IOException {
    System.out.println("Preparing " + this.getClass().getName());

    File[] directories = checkNotNull(new File(_options._corpusPrefix)).listFiles();
    List<Set<String>> intermediateAdjacencyList = new ArrayList<Set<String>>(directories.length);
    adjacencyList = new ArrayList<Set<Integer>>(directories.length);
    _documents = HashBiMap.create(directories.length);
    int docId = 0;

    for(File document : directories) {
      System.out.println("Preparing document " + docId);
      HeuristicLinkExtractor extractor = new HeuristicLinkExtractor(document);
      Set<String> outLinks = new HashSet<String>();

      _documents.put(extractor.getLinkSource(), docId++);
      String nextLink = extractor.getNextInCorpusLinkTarget();
      while(nextLink != null) {
        outLinks.add(nextLink);
        nextLink = extractor.getNextInCorpusLinkTarget();
      }
      intermediateAdjacencyList.add(outLinks);
    }

    for(Set<String> outLinks : intermediateAdjacencyList) {
      Set<Integer> converted = new HashSet<Integer>();
      for(String outLink : outLinks) {
        if(_documents.containsKey(outLink)) {
          converted.add(_documents.get(outLink));
        }
      }
      outLinks.clear();
      adjacencyList.add(converted);
    }
  }

  /**
   * This function computes the PageRank based on the internal graph generated
   * by the {@link prepare} function, and stores the PageRank to be used for
   * ranking.
   * 
   * Note that you will have to store the computed PageRank with each document
   * the same way you do the indexing for HW2. I.e., the PageRank information
   * becomes part of the index and can be used for ranking in serve mode. Thus,
   * you should store the whatever is needed inside the same directory as
   * specified by _indexPrefix inside {@link super._options}.
   *
   * @throws IOException
   */
  @Override
  public void compute() throws IOException {
    System.out.println("Computing using " + this.getClass().getName());
    AdjustedTransitionMatrix M = new AdjustedTransitionMatrix(adjacencyList, DAMPENING_FACTOR);
    double[] pageRank = new double[_documents.size()];
    for(int i = 0; i < pageRank.length; i++) {
      pageRank[i] = 1.0 / pageRank.length;
    }
    pageRank = M.times(pageRank);
    pageRank = M.times(pageRank);
  }

  /**
   * During indexing mode, this function loads the PageRank values computed
   * during mining mode to be used by the indexer.
   *
   * @throws IOException
   */
  @Override
  public Object load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    return null;
  }

  class TransitionMatrix {
    private List<Set<Integer>> _adjacencyList;
    protected int n;

    public TransitionMatrix(List<Set<Integer>> adjacencyList) {
      _adjacencyList = adjacencyList;
      n = adjacencyList.size();
    }

    public double elem(int row, int col) {
      checkElementIndex(row, n);
      checkElementIndex(col, n);
      Set<Integer> outLinks = _adjacencyList.get(col);
      if(outLinks.contains(row)) {
        return 1.0 / outLinks.size();
      } else {
        return 0;
      }
    }

    public double[] times(double[] vector) {
      checkArgument(vector.length == n);
      double[] result = new double[n];
      for(int i = 0; i < n; i++) {
        for(int j = 0; j < n; j++) {
          result[i] += elem(i, j) * vector[j];
        }
      }
      return result;
    }
  }

  class AdjustedTransitionMatrix extends TransitionMatrix {
    private double lambda;

    public AdjustedTransitionMatrix(List<Set<Integer>> adjacencyList, double dampeningFactor) {
      super(adjacencyList);
      lambda = dampeningFactor;
    }

    @Override
    public double elem(int row, int col) {
      double transitionMatrixValue = super.elem(row, col);
      return (1.0 - lambda) * transitionMatrixValue + lambda / n;
    }
  }
}
