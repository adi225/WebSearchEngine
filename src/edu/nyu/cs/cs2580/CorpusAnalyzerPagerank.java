package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import static com.google.common.base.Preconditions.*;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {

  private static final double DAMPENING_FACTOR = 0.1;
  private static final int WRITER_BUFFER_SIZE = 50000000;
  private static final int READER_BUFFER_SIZE = 5000000;

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
    File intermediateAdjacencyListFile = new File(_options._indexPrefix + "/.adjacencyList");
    PrintWriter intermediateAdjacencyListWriter =
            new PrintWriter(new BufferedWriter(new FileWriter(intermediateAdjacencyListFile), WRITER_BUFFER_SIZE));

    _documents = HashBiMap.create(directories.length);
    int docId = 0;

    for(File document : directories) {
      System.out.println("Preparing document " + docId);
      HeuristicLinkExtractor extractor = new HeuristicLinkExtractor(document);

      _documents.put(extractor.getLinkSource(), docId++);
      intermediateAdjacencyListWriter.print(extractor.getLinkSource() + " ");
      String nextLink = extractor.getNextInCorpusLinkTarget();
      while(nextLink != null) {
        intermediateAdjacencyListWriter.print(nextLink + " ");
        nextLink = extractor.getNextInCorpusLinkTarget();
      }
      intermediateAdjacencyListWriter.println();
    }
    intermediateAdjacencyListWriter.close();

    adjacencyList = new ArrayList<Set<Integer>>(directories.length);
    Scanner intermediateAdjacencyListScanner =
            new Scanner(new BufferedReader(new FileReader(intermediateAdjacencyListFile), READER_BUFFER_SIZE));

    while (intermediateAdjacencyListScanner.hasNextLine()) {
      String[] tokens = intermediateAdjacencyListScanner.nextLine().split(" ");
      Set<Integer> outLinks = Sets.newHashSet();
      checkState(_documents.containsKey(tokens[0]));
      checkState(_documents.get(tokens[0]) == adjacencyList.size());
      for(int i = 1; i < tokens.length; i++) {
        if(_documents.containsKey(tokens[i])) {
          outLinks.add(_documents.get(tokens[i]));
        }
      }
      adjacencyList.add(outLinks);
    }
    intermediateAdjacencyListScanner.close();
    intermediateAdjacencyListFile.delete();
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
    TransitionMatrix M = new TransitionMatrix(adjacencyList);
    double[] pageRank = new double[_documents.size()];
    for(int i = 0; i < pageRank.length; i++) {
      pageRank[i] = 1.0 / pageRank.length;
    }
    pageRank = M.iteratePageRank(pageRank);
    pageRank = M.iteratePageRank(pageRank);

    int maxPRIndex = -1;
    double maxPR = -1;
    for(int i = 0; i < pageRank.length; i++) {
      if(maxPR < pageRank[i]) {
        maxPR = pageRank[i];
        maxPRIndex = i;
      }
    }
    System.out.println("Max PR doc: " + _documents.inverse().get(maxPRIndex));
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
      System.out.print("Multiplying matrix.");
      checkArgument(vector.length == n);
      double[] result = new double[n];
      for(int i = 0; i < n; i++) {
        for(int j = 0; j < n; j++) {
          result[i] += elem(i, j) * vector[j];
        }
        if(i % (n / 5) == 0) {
          System.out.print(".");
        }
      }
      System.out.println(".");
      return result;
    }

    public double[] iteratePageRank(double[] vector) {
      vector = this.times(vector);
      for(int i = 0; i < vector.length; i++) {
        vector[i] = (1.0 - DAMPENING_FACTOR) * vector[i] + DAMPENING_FACTOR / vector.length;
      }
      return vector;
    }
  }
}
