package edu.nyu.cs.cs2580;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  @Test
  public void testIntToByteConversion() throws Exception {
    for(int i = 0; i < 10000000; i++) {
      byte[] encoded = VByteUtils.intToBytes(i);
      int decoded = VByteUtils.bytesToInt(encoded);
      assertEquals(4, encoded.length);
      assertEquals(i, decoded);
    }
  }

  @Test
  public void testIntToByteStreamReader() throws Exception {
    for(int i = 0; i < 10000000; i++) {
      byte[] encoded = VByteUtils.intToBytes(i);
      DataInputStream is = new DataInputStream(new ByteArrayInputStream(encoded));
      int decoded = is.readInt();
      assertEquals(4, encoded.length);
      assertEquals(i, decoded);
    }
  }

  @Test
  public void testIntegerListToByteListConverter() throws Exception {
    Map<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
    List<Integer> a = Lists.newArrayList(1, 54, 657, 7676, 76577, 567657, 67578548);
    List<Integer> b = Lists.newArrayList(5, 76, 676, 3278, 84856, 285125, 54845518);
    map.put(5, a);
    map.put(621, b);
    Map<Integer, List<Integer>> map2 = Maps.newHashMap(map);
    Map<Integer, List<Byte>> converted = VByteUtils.integerPostingListAsBytes(map);
    for(int key : converted.keySet()) {
      List<Byte> list = converted.get(key);
      int i = 0;
      for(int number : map2.get(key)) {
        byte[] bytes = new byte[4];
        bytes[0] = list.get(i++);
        bytes[1] = list.get(i++);
        bytes[2] = list.get(i++);
        bytes[3] = list.get(i++);
        int decoded = VByteUtils.bytesToInt(bytes);
        assertEquals(number, decoded);
      }
    }
  }

}