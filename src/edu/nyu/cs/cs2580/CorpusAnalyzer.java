package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * This is the abstract CorpusAnalyzer class for all concrete CorpusAnalyzer
 * implementations. The goal of the CorpusAnalyzer is to complement the Indexer
 * and compute various document-level quality measures such as PageRank.
 * 
 * In HW3: students will implement {@link CorpusAnalyzerPagerank}.
 * 
 * @author congyu
 */
public abstract class CorpusAnalyzer {
  /**
   * A simple link extractor that extracts links from provided documents within
   * the corpus. It only extracts links pointing to documents "possibly" within
   * the homework corpus. The pointees of those links are only "possibly" within
   * the homework corpus because they may not have been crawled by our simple
   * crawler.
   * 
   * In HW3: use this class to process the documents for link extraction.
   * 
   * This class is not thread-safe.
   * 
   * @author congyu
   */
  protected static class HeuristicLinkExtractor {
    private static final Pattern LINK_PATTERN =
        Pattern.compile("<[a|A].*?href=\"([^ /#]*)\".*?>");

    private String _linkSource = null;
    private BufferedReader _reader = null;
    private Matcher _matcher = null;

    // Constructs the extractor based on the content of the provided file.
    public HeuristicLinkExtractor(File file) throws IOException {
      _linkSource = file.getName();
      _reader = new BufferedReader(new FileReader(file));
      String line = _reader.readLine();
      if (line != null) {
        _matcher = LINK_PATTERN.matcher(line);
      }
    }

    // Returns the simple file name as the link source.
    public String getLinkSource() {
      return _linkSource;
    }

    // Finds the target for the next link from the file and returns its simple
    // file name. Returns null if not more links are found. The returned link
    // is extracted heuristically and may not necessarily appear in the corpus.
    public String getNextInCorpusLinkTarget() throws IOException {
      if (_matcher == null) {  // Not initialized
        return null;
      }
      String linkTarget = null;
      while (linkTarget == null) {
        if (_matcher.find()) {
          if ((linkTarget = _matcher.group(1)) != null) {
            return linkTarget;
          }
        }
        String line = _reader.readLine();
        if (line == null) {  // End of file
          _matcher = null;
          _reader.close();
          break;
        }
        _matcher = LINK_PATTERN.matcher(line);
      }
      return linkTarget;
    }
  };

  // Utility for ignoring hidden files in the file system.
  protected static boolean isValidDocument(File file) {
    return !file.getName().startsWith(".");  // Remove hidden files.
  }
  
  // Options to configure each concrete CorpusAnalyzer.
  protected Options _options = null;
  
  public CorpusAnalyzer(Options options) {
    _options = options;
  }
  
  // Processes the corpus and prepare necessary internal data structure for the
  // compute function below.
  public abstract void prepare() throws IOException;

  // Computes the desired measure based on the internal data structure created
  // by the prepare function above. Store the results to be used by Indexer in
  // the load function below.
  public abstract void compute() throws IOException;

  // Loads the stored analysis results computed by the compute function above.
  // Called during indexing mode.
  public abstract Object load() throws IOException;

  /**
   * All CorpusAnalyzers must be created through this factory class based on
   * the provided {@code options}.
   */
  public static class Factory {
    public static CorpusAnalyzer getCorpusAnalyzerByOption(Options options) {
      if (options._corpusAnalyzerType.equals("pagerank")) {
        return new CorpusAnalyzerPagerank(options);
      }
      return null;
    }
  }
}
