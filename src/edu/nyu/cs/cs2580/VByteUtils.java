package edu.nyu.cs.cs2580;

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

}
