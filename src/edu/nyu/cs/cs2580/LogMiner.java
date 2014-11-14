package edu.nyu.cs.cs2580;

import java.io.IOException;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * This is the abstract LogMiner class for all concrete LogMiner
 * implementations. The goal of the LogMiner is to complement the Indexer and
 * compute various document-level quality measures such as NumViews from log.
 * 
 * In HW3: students will implement {@link LogMinerNumviews}.
 * 
 * @author congyu
 */
public abstract class LogMiner {
  // Options to configure each concrete LogMiner.
  protected Options _options = null;
  
  public LogMiner(Options options) {
    _options = options;
  }

  // Computes the desired measure based on the log and store the results to be
  // used by the load function below.
  public abstract void compute() throws IOException;

  // Loads the stored mining results computed by the compute function above.
  // Called during indexing mode.
  public abstract Object load() throws IOException;

  /**
   * All LogMiners must be created through this factory class based on
   * the provided {@code options}.
   */
  public static class Factory {
    public static LogMiner getLogMinerByOption(Options options) {
      if (options._logMinerType.equals("numviews")) {
        return new LogMinerNumviews(options);
      }
      return null;
    }
  }
}
