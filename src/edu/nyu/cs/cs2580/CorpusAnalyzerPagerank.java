package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import static com.google.common.base.Preconditions.*;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {

  private static final float DAMPENING_FACTOR = 0.1f;
  private static final int WRITER_BUFFER_SIZE = 50000000;
  private static final int READER_BUFFER_SIZE = 5000000;
  private final String pageRankFilePath;

  protected BiMap<String, Integer> _documents;
  protected List<Set<Integer>> invertedAdjacencyList;
  Map<String, String> _redirects;
  protected int[] outlinks;

  public CorpusAnalyzerPagerank(Options options) {
    super(options);
    pageRankFilePath = _options._indexPrefix + "/pagerank";
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
    outlinks = new int[directories.length];
    int docId = 0;
    _redirects = Maps.newHashMap();

    for(File document : directories) {
      System.out.println("Preparing document " + docId);

      HeuristicLinkExtractor extractor = new HeuristicLinkExtractor(document);
      String nextLink = extractor.getNextInCorpusLinkTarget();
      if(extractor.getRedirect() != null) {
        _redirects.put(extractor.getLinkSource(), extractor.getRedirect());
      } else {
        _documents.put(extractor.getLinkSource(), docId++);
        intermediateAdjacencyListWriter.print(extractor.getLinkSource() + " ");
        while (nextLink != null) {
          intermediateAdjacencyListWriter.print(nextLink + " ");
          nextLink = extractor.getNextInCorpusLinkTarget();
        }
        intermediateAdjacencyListWriter.println();
      }
      if(docId >= IndexerInverted.MAX_DOCS) break;
    }
    intermediateAdjacencyListWriter.close();

    invertedAdjacencyList = new ArrayList<Set<Integer>>(directories.length);
    for(int i = 0; i < docId; ++i) {
      invertedAdjacencyList.add(new HashSet<Integer>());
    }
    Scanner intermediateAdjacencyListScanner =
            new Scanner(new BufferedReader(new FileReader(intermediateAdjacencyListFile), READER_BUFFER_SIZE));

    while (intermediateAdjacencyListScanner.hasNextLine()) {
      String[] tokens = intermediateAdjacencyListScanner.nextLine().split(" ");
      checkState(_documents.containsKey(tokens[0]));
      int fromDocId = _documents.get(tokens[0]);
      for(int i = 1; i < tokens.length; i++) {
        while(_redirects.containsKey(tokens[i])) {
          tokens[i] = _redirects.get(tokens[i]);
        }
        if(_documents.containsKey(tokens[i])) {
          int toDocId = _documents.get(tokens[i]);
          invertedAdjacencyList.get(toDocId).add(fromDocId);
          ++outlinks[fromDocId];
        }
      }
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
    TransitionMatrix M = new TransitionMatrix(invertedAdjacencyList, outlinks);
    float[] pageRank = new float[_documents.size()];
    for(int i = 0; i < pageRank.length; i++) {
      pageRank[i] = 1.0f / pageRank.length;
    }
    pageRank = M.iteratePageRank(pageRank);
    pageRank = M.iteratePageRank(pageRank);

    File pageRankFile = new File(pageRankFilePath);
    PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(pageRankFile)));
    for(int i = 0; i < pageRank.length; i++) {
      writer.println(_documents.inverse().get(i) + " " + pageRank[i]);
    }
    writer.close();
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
    File pageRankFile = new File(pageRankFilePath);
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(pageRankFile)));
    Map<String, Float> pageRank = Maps.newHashMap();
    while (scanner.hasNextLine()){
      String[] tokens = scanner.nextLine().split(" ");
      pageRank.put(tokens[0], Float.parseFloat(tokens[1]));
    }
    return pageRank;
  }

  public class TransitionMatrix {
    private List<Set<Integer>> _invertedAdjacencyList;
    private int[] _outlinks;
    protected int n;

    public TransitionMatrix(List<Set<Integer>> invertedAdjacencyList, int[] outlinks) {
      _invertedAdjacencyList = invertedAdjacencyList;
      _outlinks = outlinks;
      n = invertedAdjacencyList.size();
    }

    public float[] times(float[] vector) {
      System.out.print("Multiplying matrix.");
      checkArgument(vector.length == n);
      float[] result = new float[n];
      for(int i = 0; i < n; i++) {
        Set<Integer> inlinks = _invertedAdjacencyList.get(i);
        for(int inlink : inlinks) {
          float elem = 1.0f / _outlinks[inlink];
          result[i] += elem * vector[inlink];
        }
        if((5*i) % n == 0) {
          System.out.print(".");
        }
      }
      System.out.println(".");
      return result;
    }

    public float[] iteratePageRank(float[] vector) {
      vector = this.times(vector);
      for(int i = 0; i < vector.length; i++) {
        vector[i] = (1.0f - DAMPENING_FACTOR) * vector[i] + DAMPENING_FACTOR / vector.length;
      }
      return vector;
    }
  }
}
