package edu.nyu.cs.cs2580;

import java.io.*;
import java.net.URLDecoder;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.nyu.cs.cs2580.SearchEngine.Options;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @CS2580: Implement this class for HW3.
 */
public class LogMinerNumviews extends LogMiner {

  private static final int READER_BUFFER_SIZE = 50000000;

  protected String numViewFilePath;

  public LogMinerNumviews(Options options) {
    super(options);
    numViewFilePath = _options._indexPrefix + "/numviews";
  }

  /**
   * This function processes the logs within the log directory as specified by
   * the {@link super._options}. The logs are obtained from Wikipedia dumps and have
   * the following format per line: [language]<space>[article]<space>[#views].
   * Those view information are to be extracted for documents in our corpus and
   * stored somewhere to be used during indexing.
   *
   * Note that the log contains view information for all articles in Wikipedia
   * and it is necessary to locate the information about articles within our
   * corpus.
   *
   * @throws IOException
   */
  @Override
  public void compute() throws IOException {
    System.out.println("Computing using " + this.getClass().getName());
    Map<String, Integer> numViews = Maps.newHashMap();
 
    String[] directories = checkNotNull(new File(_options._corpusPrefix)).list();
    Set<String> dirs = Sets.newHashSet(directories);
    Set<String> excludedDocuments = Sets.newHashSet();

    File[] logFilesDirectory = checkNotNull(new File(_options._logPrefix)).listFiles();
    for(File logFile : logFilesDirectory) {
      Scanner scanner = new Scanner(new BufferedReader(new FileReader(logFile), READER_BUFFER_SIZE));
      while(scanner.hasNextLine()) {
        String[] tokens = scanner.nextLine().split(" ");
        if(tokens.length != 3) {
          continue;
        }

        // Handle redirect.
        if(dirs.contains(tokens[1])) {
          File file = new File(_options._corpusPrefix + "/" + tokens[1]);
          CorpusAnalyzer.HeuristicLinkExtractor extractor = new CorpusAnalyzer.HeuristicLinkExtractor(file);
          extractor.getNextInCorpusLinkTarget();
          while(extractor.getRedirect() != null) {
            excludedDocuments.add(tokens[1]);
            tokens[1] = extractor.getRedirect();
            file = new File(_options._corpusPrefix + "/" + tokens[1]);
            extractor = new CorpusAnalyzer.HeuristicLinkExtractor(file);
            extractor.getNextInCorpusLinkTarget();
          }
        }

        try {
          int count = Integer.parseInt(tokens[2]);
          if(numViews.keySet().contains(tokens[1])) {
            numViews.put(tokens[1], numViews.get(tokens[1]) + count);
          } else if(dirs.contains(tokens[1])) {
            numViews.put(tokens[1], count);
          }
        } catch (NumberFormatException e) {
          continue;
        }
      }
      scanner.close();
    }

    for(String document : directories) {
      File file = new File(_options._corpusPrefix + "/" + document);
      CorpusAnalyzer.HeuristicLinkExtractor extractor = new CorpusAnalyzer.HeuristicLinkExtractor(file);
      extractor.getNextInCorpusLinkTarget();
      while(extractor.getRedirect() != null) {
        excludedDocuments.add(document);
        document = extractor.getRedirect();
        file = new File(_options._corpusPrefix + "/" + document);
        extractor = new CorpusAnalyzer.HeuristicLinkExtractor(file);
        extractor.getNextInCorpusLinkTarget();
      }
    }

    for(String document : directories) {
      if(!numViews.containsKey(document) && !excludedDocuments.contains(document)) {
        numViews.put(document, 0);
      }
    }

    File numViewsFile  = new File(numViewFilePath);
    PrintWriter numViewsFileWriter =
            new PrintWriter(new BufferedWriter(new FileWriter(numViewsFile)));
    for(String document : numViews.keySet()) {
      numViewsFileWriter.println(document + " " + numViews.get(document));
    }
    numViewsFileWriter.close();
  }

  /**
   * During indexing mode, this function loads the NumViews values computed
   * during mining mode to be used by the indexer.
   * 
   * @throws IOException
   */
  @Override
  public Object load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    File numViewsFile = new File(numViewFilePath);
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(numViewsFile)));
    Map<String, Integer> numViews = Maps.newHashMap();
    while (scanner.hasNextLine()){
      String[] tokens = scanner.nextLine().split(" ");
      numViews.put(tokens[0], Integer.parseInt(tokens[1]));
    }
    return numViews;
  }
}
