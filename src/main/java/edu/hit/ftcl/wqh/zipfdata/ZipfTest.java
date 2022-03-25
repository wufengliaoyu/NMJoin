package edu.hit.ftcl.wqh.zipfdata;

import org.junit.Test;

import java.util.HashMap;
import java.util.Set;

public class ZipfTest {
    @Test
    public void test() {
        ZipfGenerator zipfGenerator = new ZipfGenerator(1000, 0.2);
        HashMap<Integer, Integer> integerIntegerHashMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100000; i++) {
            int next = zipfGenerator.next();
            if (integerIntegerHashMap.containsKey(next)) {
                int i1 = integerIntegerHashMap.get(next) + 1;
                integerIntegerHashMap.put(next, i1);
            } else {
                integerIntegerHashMap.put(next,1);
            }
           // System.out.println(next);
        }

        Set<Integer> integers = integerIntegerHashMap.keySet();
        for (Integer integer : integers) {
            System.out.println(""+integer+"  -- "+integerIntegerHashMap.get(integer));
        }


    }
}
