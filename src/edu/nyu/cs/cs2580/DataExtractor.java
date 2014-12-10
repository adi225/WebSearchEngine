package edu.nyu.cs.cs2580;

import java.io.*;
import org.apache.commons.io.FileUtils;

public class DataExtractor {
  static String[] fileName;

  public static void main(String[] args) throws Exception {

    File source = new File(args[0]);
    File target = new File(args[1]);

    extractFiles(source, target);
  }

  private static void extractFiles (File source, File target) throws IOException {
    File[] directoryListing = source.listFiles();
    for (File child : directoryListing) {
      if (child.isDirectory()){
        extractFiles(child, target);
      } else {
        FileUtils.copyFileToDirectory(child, target);
      }
    }
  }
}
