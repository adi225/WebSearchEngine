package edu.nyu.cs.cs2580;

import org.junit.Test;

import static org.junit.Assert.*;

public class VByteUtilsTests {

  @Test
  public void testEncodeAndDecode() throws Exception {
    for(int i = 0; i < 10000000; i++) {
      byte[] encoded = VByteUtils.encodeInt(i);
      int decoded = VByteUtils.decodeByteArray(encoded);
      assertEquals(i, decoded);
      if(i < 128) {
        //System.out.println("Number: " + i);
        assertEquals(1, encoded.length);
      } else if (i < 16384) {
        assertEquals(2, encoded.length);
      } else if (i < 2097152) {
        assertEquals(3, encoded.length);
      } else if (i < 268435456) {
        assertEquals(4, encoded.length);
      }
      assertTrue(encoded.length <= 4);
    }
  }
}