package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.*;

/**
 * Created by andreidinuionita on 10/16/14.
 */
public class VByteUtils {

  protected static byte[] encodeInt(int number) {
    checkArgument(number >= 0, "Negative integers are not supported.");

    // Calculate number of bits required.
    int bits = 0;
    int digits = number;
    while(digits != 0) {
      digits = digits >>> 1;
      bits++;
    }

    // Calculate number of bytes required.
    int numBytes = (bits - 1) / 7 + 1;

    byte[] encodedValue = new byte[numBytes];
    for(int i = encodedValue.length - 1; i >= 0 ; i--) {
      byte b = (byte)(number & 127); // apply bitmask to get last 7 bits
      number = number >>> 7; // shift number past the 7 bits that were processed
      if(i == encodedValue.length - 1) {
        b = (byte)(b | 128); // set first bit to 1 for last byte of array
      }
      encodedValue[i] = b;
    }
    return encodedValue;
  }

  protected static int decodeByteArray(byte[] bytes) {
    int decodedValue = 0;
    for (int i = 0; i < bytes.length; i++) {
      int contribution = bytes[i] & 127; // apply bitmask to get last 7 bits only
      decodedValue = decodedValue << 7; // shift number to make 7 bits available
      decodedValue += contribution; // add 7 bits to number
      if(bytes[i] >>> 7 == 1) {
        break;
      }
    }
    return decodedValue;
  }

  protected static byte[] intToBytes(int i) {
    byte[] result = new byte[4];
    result[0] = (byte) (i >>> 24);
    result[1] = (byte) (i >>> 16);
    result[2] = (byte) (i >>> 8);
    result[3] = (byte) (i);
    return result;
  }

  protected static int bytesToInt(byte[] bytes) {
    checkArgument(bytes.length == 4);
    int i = 0;
    for(byte aByte : bytes) {
      int bByte = (int)aByte & 255; // careful converting byte with leading 1 to int (2's complement conversion)
      i = (i << 8);
      i = i | bByte;
    }
    return i;
  }

  protected static Map<Integer, List<Byte>> integerPostingListAsBytes(Map<Integer, List<Integer>> partialIndex) {
    boolean canRemove = partialIndex.get(partialIndex.keySet().iterator().next()) instanceof LinkedList;
    // TODO Remove debug code.
    boolean isTest = partialIndex instanceof IndexerInvertedCompressedTest.UnshrinkableHashMap;
    Map<Integer, List<Byte>> partialIndexAsBytes = new HashMap<Integer, List<Byte>>();
    for (Integer key : partialIndex.keySet()) {
      List<Integer> list = partialIndex.get(key);
      LinkedList<Byte> listAsBytes = new LinkedList<Byte>();
      for (int i = list.size() - 1; i >= 0; i--) {
        int number = list.get(i);
        if(canRemove && !isTest) list.remove(i);
        byte[] bytes = VByteUtils.intToBytes(number);
        listAsBytes.addFirst(bytes[3]);
        listAsBytes.addFirst(bytes[2]);
        listAsBytes.addFirst(bytes[1]);
        listAsBytes.addFirst(bytes[0]);
      }
      partialIndex.put(key, null);
      partialIndexAsBytes.put(key, listAsBytes);
    }
    partialIndex.clear();
    return partialIndexAsBytes;
  }

  protected static List<Integer> bytesPostingListAsIntegers(List<Byte> compressedList) {
    List<Integer> postingsList = new LinkedList<Integer>();
    int bytesRead = 0;
    boolean keepGoing;
    Iterator<Byte> iterator = compressedList.iterator();
    while(bytesRead < compressedList.size()) {
      int pos = 0;
      byte[] buf = new byte[8];
      keepGoing = true;
      do {
        buf[pos] = iterator.next();
        bytesRead++;
        keepGoing = (buf[pos++] & 128) == 0;
      } while(keepGoing);
      byte[] asBytes = new byte[pos];
      for(int i = 0; i < asBytes.length; i++) {
        asBytes[i] = buf[i];
      }
      postingsList.add(VByteUtils.decodeByteArray(asBytes));
    }
    return postingsList;
  }

  protected static Map<Integer, List<Integer>> bytesPostingListAsIntegers(Map<Integer, List<Byte>> partialIndex) {
    boolean canRemove = partialIndex.get(partialIndex.keySet().iterator().next()) instanceof LinkedList;
    // TODO Remove debug code.
    boolean isTest = partialIndex instanceof IndexerInvertedCompressedTest.UnshrinkableHashMap;
    Map<Integer, List<Integer>> partialIndexAsIntegers = new HashMap<Integer, List<Integer>>();
    for (Integer key : partialIndex.keySet()) {
      List<Byte> list = partialIndex.get(key);
      partialIndex.put(key, null);
      partialIndexAsIntegers.put(key, bytesPostingListAsIntegers(list));
    }
    partialIndex.clear();
    return partialIndexAsIntegers;
  }

  protected static List<Integer> deltaPostingsListToSequentialPostingsList(List<Integer> deltaPostingsListArg) throws IOException {
    LinkedList<Integer> deltaPostingsList = (LinkedList<Integer>) deltaPostingsListArg;
    List<Integer> postingsList = new LinkedList<Integer>();

    int _prevDocId = 0;
    while(deltaPostingsList.size() > 0) {
      _prevDocId += deltaPostingsList.pollFirst();
      postingsList.add(_prevDocId);

      int _prevOccurance = 0;
      int numOccurances = deltaPostingsList.pollFirst();
      postingsList.add(numOccurances);
      for(int i = 0; i < numOccurances; i++) {
        _prevOccurance += deltaPostingsList.pollFirst();;
        postingsList.add(_prevOccurance);
      }
    }
    return postingsList;
  }
}
