package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
      byte b = (byte)(number & 0b01111111); // apply bitmask to get last 7 bits
      number = number >>> 7; // shift number past the 7 bits that were processed
      if(i == encodedValue.length - 1) {
        b = (byte)(b | 0b10000000); // set first bit to 1 for last byte of array
      }
      encodedValue[i] = b;
    }
    return encodedValue;
  }

  protected static int decodeByteArray(byte[] bytes) {
    int decodedValue = 0;
    for (int i = 0; i < bytes.length; i++) {
      int contribution = bytes[i] & 0b01111111; // apply bitmask to get last 7 bits only
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
      int bByte = (int)aByte & 0b11111111; // careful converting byte with leading 1 to int (2's complement conversion)
      i = (i << 8);
      i = i | bByte;
    }
    return i;
  }

  protected static Map<Integer, List<Byte>> integerPostingListAsBytes(Map<Integer, List<Integer>> partialIndex) {
    boolean canRemove = partialIndex.get(partialIndex.keySet().iterator().next()) instanceof LinkedList;
    Map<Integer, List<Byte>> partialIndexAsBytes = new HashMap<Integer, List<Byte>>();
    for (Integer key : partialIndex.keySet()) {
      List<Integer> list = partialIndex.get(key);
      LinkedList<Byte> listAsBytes = new LinkedList<Byte>();
      for (int i = list.size() - 1; i >= 0; i--) {
        int number = list.get(i);
        if(canRemove) list.remove(i);
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
}
