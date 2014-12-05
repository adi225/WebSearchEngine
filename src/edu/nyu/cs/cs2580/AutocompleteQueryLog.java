package edu.nyu.cs.cs2580;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.*;

/**
 * Created by andreidinuionita on 12/1/14.
 */
public class AutocompleteQueryLog {

  private static final AutocompleteQueryLog _singletonInstance = new AutocompleteQueryLog();

  private AutocompleteQueryLog() {}
  public static AutocompleteQueryLog getInstance() {
    return _singletonInstance;
  }

  private String _autocompletePrefix = "data/AOL/";
  private String _mainFileName = "autocomplete.log";
  private String _mainFile = _autocompletePrefix + _mainFileName;
  private static final int CHUNK_SIZE = 1000000;
  private static final int SEARCH_UNIT_SIZE = 1000;

  public static void main(String[] args) throws IOException {
    AutocompleteQueryLog ql = AutocompleteQueryLog.getInstance();
    ql.loadAutocomplete();
    System.out.println("Loaded autocomplete log.");
    List<String> suggestions = ql.topAutoCompleteSuggestions("how", 500);
    for(String suggestion : suggestions) {
      System.out.println(suggestion);
    }
  }

  private List<Map.Entry<String, Long>> _skipPointer = Lists.newArrayList();

  public void prepareMainFile() throws IOException {
    System.out.println("Preparing autocomplete file.");
    String newMainFile = _mainFileName + System.currentTimeMillis();

    // Do the work.
    sortQueries(_mainFile, ".sorted");
    consolidateQueries(".sorted", newMainFile);

    // Cleanup and swap the main file.
    new File(_autocompletePrefix + ".sorted").delete();
    String prevMainFile = _mainFile;
    _mainFile = _autocompletePrefix + newMainFile;
    new File(prevMainFile).delete();
    new File(_mainFile).renameTo(new File(_mainFileName));
    _mainFile = _autocompletePrefix + _mainFileName;
  }

  public void recordQuery(String query) {
    query = cleanQuery(query);
    try {
      PrintWriter writer = new PrintWriter(new FileWriter(new File(_mainFile), true));
      writer.print("\n" + query);
    } catch (IOException e) {}
  }

  public void loadAutocomplete() throws IOException {
    Scanner scanner = new Scanner(new BufferedReader(new FileReader(new File(_mainFile)), CHUNK_SIZE));
    long bytesRead = 0;
    for (int i = 0; scanner.hasNext(); ++i) {
      String line = scanner.nextLine();
      bytesRead += (line + "\n").getBytes().length;
      if (i % SEARCH_UNIT_SIZE == 0) {
        String[] tokens = line.split("\t");
        _skipPointer.add(Maps.immutableEntry(tokens[0], bytesRead));
      }
    }
  }

  public List<String> topAutoCompleteSuggestions(String query, int maxNum) throws IOException {
    checkArgument(maxNum > 0, "Max num must be > 0.");
    long start = System.currentTimeMillis();
    query = cleanQuery(checkNotNull(query));
    int i = 0;
    while (i < _skipPointer.size() && _skipPointer.get(i).getKey().compareTo(query) < 0) {
      ++i; // traverse until you pass your target
    }
    --i; // go one back to make sure you include your target.

    FileInputStream readerFIS = new FileInputStream(_mainFile);
    readerFIS.skip(_skipPointer.get(i).getValue());
    BufferedReader reader = new BufferedReader(new InputStreamReader(readerFIS), CHUNK_SIZE);
    String line;
    System.out.println("Time to find index: " + (System.currentTimeMillis() - start));
    while((line = reader.readLine()) != null && !line.startsWith(query)) {
      continue;
    }
    System.out.println("Time to find section: " + (System.currentTimeMillis() - start));

    Comparator<Map.Entry<String,Integer>> byMapValues = new Comparator<Map.Entry<String,Integer>>() {
      @Override
      public int compare(Map.Entry<String,Integer> left, Map.Entry<String,Integer> right) {
        return left.getValue().compareTo(right.getValue());
      }
    };
    TreeSet<Map.Entry<String,Integer>> preliminaryResults = new TreeSet<Map.Entry<String, Integer>>(byMapValues);

    while((line = reader.readLine()) != null && line.startsWith(query)) {
      String[] tokens = line.split("\t");
      int score = (tokens.length == 2) ? Integer.parseInt(tokens[1]) : 1;
      preliminaryResults.add(Maps.immutableEntry(tokens[0], score));
      if(preliminaryResults.size() > maxNum) {
        preliminaryResults.pollFirst(); // lowest element
      }
    }
    reader.close();

    List<String> results = Lists.newArrayList();
    while (preliminaryResults.size() > 0) {
      String result = preliminaryResults.pollLast().getKey(); // highest element
      results.add(result.substring(query.length()));
    }
    System.out.println("Time: " + (System.currentTimeMillis() - start));
    return results;
  }

  private static void consolidateQueries(String originalFile, String resultFile) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(new File("data/AOL/" + originalFile)));
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File("data/AOL/" + resultFile)));

    String line;
    String prevLine = "";
    int score = 0;
    while((line = reader.readLine()) != null) {
      String[] tokens = line.split("\t");
      checkState(tokens.length < 3);
      int currentScore = (tokens.length == 2) ? Integer.parseInt(tokens[1]) : 1;
      if(tokens[0].equals(prevLine)) {
        score += currentScore;
      } else {
        if(!prevLine.isEmpty()) {
          writer.write(prevLine + "\t" + score + "\n");
        }
        score = currentScore;
      }
      prevLine = tokens[0];
    }
    writer.write(prevLine + "\t" + score + "\n");
    reader.close();
    writer.close();
  }

  private static void sortQueries(String originalFile, String resultFile) throws IOException {
    resultFile = "data/AOL/" + resultFile;
    Path originalLogPath = FileSystems.getDefault().getPath("data/AOL", originalFile);
    Path queryLogPath = FileSystems.getDefault().getPath("data/AOL", "trial.txt");
    Files.copy(originalLogPath, queryLogPath, REPLACE_EXISTING);

    File queryLog = queryLogPath.toFile();

    System.out.println("Total length: " + queryLog.length());
    int sortedPointer = 0;
    while (sortedPointer < queryLog.length() - 10000) {
      BufferedReader reader = new BufferedReader(new FileReader(queryLog));
      reader.skip(sortedPointer);
      LinkedList<String> queries = new LinkedList<String>();
      LinkedList<String> existing = new LinkedList<String>();
      int charsRead = 0;
      for(int i = 0; i < CHUNK_SIZE; i++) {
        String line = cleanQuery(reader.readLine());
        if(line == null || line.isEmpty()) continue;
        queries.add(line);
        charsRead += (line + "\n").toCharArray().length;
      }
      reader.close();
      Collections.sort(queries);

      File resultLog = new File("data/AOL/.trial_intermediary" + sortedPointer);
      BufferedReader sortedQueries = new BufferedReader(new FileReader(queryLog));
      BufferedWriter resultQueries = new BufferedWriter(new FileWriter(resultLog));
      int headCharsRead = 0;
      int charsWritten = 0;
      while(headCharsRead < sortedPointer || existing.size() > 0) {
        if(headCharsRead < sortedPointer) {
          String currentLine = sortedQueries.readLine();
          if(currentLine == null) break;
          existing.addLast(currentLine);
          headCharsRead += (currentLine + "\n").toCharArray().length;
        }
        if(queries.size() > 0 && existing.getFirst().compareTo(queries.getFirst()) > 0) {
          String lineToWrite = queries.removeFirst() + "\n";
          resultQueries.write(checkNotNull(lineToWrite));
          charsWritten += lineToWrite.toCharArray().length;
        } else if(existing.size() > 0) {
          String lineToWrite = existing.removeFirst() + "\n";
          resultQueries.write(checkNotNull(lineToWrite));
          charsWritten += lineToWrite.toCharArray().length;
        }
      }
      while(queries.size() > 0) {
        String lineToWrite = queries.removeFirst() + "\n";
        resultQueries.write(checkNotNull(lineToWrite));
        charsWritten += lineToWrite.toCharArray().length;
      }
      sortedQueries.skip(charsRead);
      checkState(headCharsRead == sortedPointer);

      checkState(charsRead + headCharsRead == charsWritten);
      sortedPointer = charsWritten;

      char[] buf = new char[10000];
      int buflen;
      while((buflen = sortedQueries.read(buf, 0, 10000)) > 0) {
        resultQueries.write(checkNotNull(buf), 0, buflen);
      }
      sortedQueries.close();
      resultQueries.close();

      queryLog.delete();
      queryLog = resultLog;
      System.out.println("Sorted to: " + sortedPointer);
    }
    File result = new File(resultFile);
    result.createNewFile();
    queryLog.renameTo(result);
  }

  private static String cleanQuery(String query) {
    if(query == null) return null;
    String[] tokens = query.split("\t");
    checkState(tokens.length < 3);
    String[] words = tokens[0].split("[^\\p{Alnum}\\.]+");
    String result = Joiner.on(' ').skipNulls().join(words);
    result = (tokens.length == 2) ? result + "\t" + tokens[1] : result;
    return result;
  }
}
