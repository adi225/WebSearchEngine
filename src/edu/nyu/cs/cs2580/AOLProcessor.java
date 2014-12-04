package edu.nyu.cs.cs2580;

import com.google.common.base.Joiner;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.*;

/**
 * Created by andreidinuionita on 12/1/14.
 */
public class AOLProcessor {

  private static final int CHUNK_SIZE = 1000000;

  public static void main(String[] args) throws IOException {
    sortQueries("res.txt", "res2.txt");
    consolidateQueries("res2.txt", "res3.txt");
  }

  public static void consolidateQueries(String originalFile, String resultFile) throws IOException {
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

  public static void sortQueries(String originalFile, String resultFile) throws IOException {
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

  public static String cleanQuery(String query) {
    if(query == null) return null;
    String[] tokens = query.split("\t");
    checkState(tokens.length < 3);
    String[] words = tokens[0].split("[^\\p{Alnum}\\.]+");
    String result = Joiner.on(' ').skipNulls().join(words);
    result = (tokens.length == 2) ? result + "\t" + tokens[1] : result;
    return result;
  }
}
