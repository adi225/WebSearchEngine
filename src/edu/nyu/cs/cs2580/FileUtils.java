package edu.nyu.cs.cs2580;

import java.io.*;
import java.util.*;

/**
 * Created by andreidinuionita on 10/16/14.
 */
public class FileUtils {

  static class FileRange implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;
    public long offset;
    public long length;

    public FileRange(long _offset, long _length) {
      offset = _offset;
      length = _length;
    }

    @Override
    public String toString() {
      return "(->" + offset + ", " + length + ")";
    }

    @Override
    public boolean equals(Object other) {
      if(other instanceof FileRange) {
        FileRange o = (FileRange)other;
        if(o.offset == this.offset && o.length == this.length) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      long res = this.offset * 37 + this.length;
      return (int)res;
    }
  }


  protected static long dumpIndexToFile(Map<Integer, List<Integer>> partialIndex, File _file) throws IOException {
    System.out.println("Generating partial index: " + _file.getAbsolutePath());
    Map<Integer, FileRange> indexPointerMap = new HashMap<Integer, FileRange>();

    // Write actual index to auxiliary file
    File aux = new File(_file.getAbsolutePath() + "_aux");
    long filePointer = 0;
    DataOutputStream auxDOS = new DataOutputStream(new FileOutputStream(aux));

    List<Integer> words = new ArrayList(partialIndex.keySet());
    Collections.sort(words);
    for (Integer word : words) {
      List<Integer> postingsList = partialIndex.get(word);
      indexPointerMap.put(word, new FileRange(filePointer, postingsList.size()));
      for (int posting : postingsList) {
        auxDOS.writeInt(posting);
      }
      filePointer += postingsList.size() * 4;
    }
    auxDOS.close();

    // Append pointer map to file before actual index.
    long offset = writeObjectToFile(indexPointerMap, _file);

    // Stream actual index from aux file to end of index file.
    appendFileToFile(aux, _file);
    return offset;
  }

  protected static long loadFromFileIntoIndex(DataInputStream _fileDIS, Map<Integer, FileRange> index) throws IOException {
    index.clear();
    try {
      List<Object> metadata = new ArrayList<Object>();
      long filePointer = readObjectsFromFileIntoList(_fileDIS, metadata);
      index.putAll((Map<Integer, FileRange>) metadata.get(0));
      return filePointer;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new IllegalArgumentException("File is not correctly deserializable.");
    }
  }

  protected static long mergeFilesIntoIndexAndFile(String file1URL,
                                                   String file2URL,
                                                   Map<Integer, FileRange> index,
                                                   String resFileURL) throws IOException {
    return mergeFilesIntoIndexAndFile(new File(file1URL), new File(file2URL), index, new File(resFileURL));
  }

  protected static long mergeFilesIntoIndexAndFile(File _file1,
                                                   File _file2,
                                                   Map<Integer, FileRange> index,
                                                   File _resFile) throws IOException {
    index.clear();
    File aux = new File(_resFile.getAbsolutePath() + "_aux");
    Map<Integer, FileRange> index1 = new HashMap<Integer, FileRange>();
    Map<Integer, FileRange> index2 = new HashMap<Integer, FileRange>();
    DataInputStream file1DIS = new DataInputStream(new FileInputStream(_file1));
    DataInputStream file2DIS = new DataInputStream(new FileInputStream(_file2));
    DataOutputStream auxDOS = new DataOutputStream(new FileOutputStream(aux));

    loadFromFileIntoIndex(file1DIS, index1);
    loadFromFileIntoIndex(file2DIS, index2);

    List<Integer> index1Words = new ArrayList<Integer>(index1.keySet());
    List<Integer> index2Words = new ArrayList<Integer>(index2.keySet());
    Collections.sort(index1Words);
    Collections.sort(index2Words);
    int index1WordsSize = index1Words.size();
    int index2WordsSize = index2Words.size();
    int totalSize = index1WordsSize + index2WordsSize;
    int i, li, ri;
    i = li = ri = 0;
    long fileOffset = 0;

    while (i < totalSize) {
      if(li < index1WordsSize && ri < index2WordsSize) {
        int word1 = index1Words.get(li);
        int word2 = index2Words.get(ri);
        if (word1 < word2) {
          FileRange word1Range = index1.get(word1);
          index.put(word1, new FileRange(fileOffset, word1Range.length));
          byte[] buf = new byte[(int)word1Range.length * 4];
          fileOffset += buf.length;
          file1DIS.read(buf);
          auxDOS.write(buf);
          li++; i++;
        } else if (word2 < word1) {
          FileRange word2Range = index2.get(word2);
          index.put(word2, new FileRange(fileOffset, word2Range.length));
          byte[] buf = new byte[(int)word2Range.length * 4];
          fileOffset += buf.length;
          file2DIS.read(buf);
          auxDOS.write(buf);
          ri++; i++;
        } else {
          FileRange word1Range = index1.get(word1);
          FileRange word2Range = index2.get(word2);
          long postingListTotalSize = word1Range.length + word2Range.length;
          index.put(word1, new FileRange(fileOffset, postingListTotalSize));
          byte[] buf1 = new byte[(int)word1Range.length * 4];
          byte[] buf2 = new byte[(int)word2Range.length * 4];
          fileOffset += buf1.length + buf2.length;
          file1DIS.read(buf1);
          file2DIS.read(buf2);
          auxDOS.write(buf1);
          auxDOS.write(buf2);
          li++; ri++; i+=2;
        }
      } else {
        if(li < index1WordsSize) {
          int word1 = index1Words.get(li);
          FileRange word1Range = index1.get(word1);
          index.put(word1, new FileRange(fileOffset, word1Range.length));
          byte[] buf = new byte[(int)word1Range.length * 4];
          fileOffset += buf.length;
          file1DIS.read(buf);
          auxDOS.write(buf);
          li++; i++;
        }
        if(ri < index2WordsSize) {
          int word2 = index2Words.get(ri);
          FileRange word2Range = index2.get(word2);
          index.put(word2, new FileRange(fileOffset , word2Range.length));
          byte[] buf = new byte[(int)word2Range.length * 4];
          fileOffset += buf.length;
          file2DIS.read(buf);
          auxDOS.write(buf);
          ri++; i++;
        }
      }
    }

    file1DIS.close();
    file2DIS.close();
    auxDOS.close();

    // Append pointer map to file before actual index.
    long offset = writeObjectToFile(index, _resFile);

    // Stream actual index from aux file to end of index file.
    appendFileToFile(aux, _resFile);

    _file1.delete();
    _file2.delete();
    return offset;
  }

  protected static <T> long writeObjectToFile(T object, File _file) throws IOException {
    List<Object> list = new ArrayList<Object>();
    list.add(object);
    return writeObjectsToFile(list, _file);
  }

  protected static long writeObjectsToFile(List<Object> stores, File _file) throws IOException {
    DataOutputStream fileDOS = new DataOutputStream(new FileOutputStream(_file, true));
    long totalSize = 0;
    for(Object store : stores) {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      ObjectOutputStream o = new ObjectOutputStream(b);
      o.writeObject(store);
      byte[] bytes = b.toByteArray();
      totalSize += bytes.length;
      fileDOS.writeInt(bytes.length);
      fileDOS.write(bytes);
    }
    fileDOS.writeInt(0);
    fileDOS.close();
    return totalSize + stores.size() * 4 + 4;
  }

  protected static long readObjectsFromFileIntoList(File file, List<Object> store) throws IOException, ClassNotFoundException {
    DataInputStream fileDIS = new DataInputStream(new FileInputStream(file));
    return readObjectsFromFileIntoList(fileDIS, store);
  }

  protected static long readObjectsFromFileIntoList(DataInputStream fileDIS, List<Object> store) throws IOException, ClassNotFoundException {
    if(store == null) throw new IllegalArgumentException("Cannot read into null list.");
    store.clear();
    int size = fileDIS.readInt();
    long totalSize = size;
    while(size > 0) {
      byte[] bytes = new byte[size];
      fileDIS.read(bytes);
      ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(bytes));
      store.add(o.readObject());
      size = fileDIS.readInt();
      totalSize += size;
    }
    return totalSize + store.size() * 4 + 4;
  }

  public static long appendFileToFile(File appendedFile, File appendToFile) throws IOException {
    FileOutputStream appendToFileOS = new FileOutputStream(appendToFile, true);
    FileInputStream appendedFileIS = new FileInputStream(appendedFile);
    long bytesWritten = copyStream(appendedFileIS, appendToFileOS);
    appendedFileIS.close();
    appendToFileOS.close();
    appendedFile.delete();
    return bytesWritten;
  }

  public static long copyStream(InputStream from, OutputStream to) throws IOException {
    final int BUF_SIZE = 0x1000; // 4K
    if(from == null) throw new IllegalArgumentException("Input Stream must be specified.");
    if(to == null) throw new IllegalArgumentException("Output Stream must be specified");
    byte[] buf = new byte[BUF_SIZE];
    long total = 0;
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
      total += r;
    }
    return total;
  }
}
