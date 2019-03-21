package org.apache.flink.cep.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CountBitmapTest {

    @Test
    public void testAdd() {
        CountBitmap countBitmap = new CountBitmap();
        countBitmap.add(1);
        countBitmap.add(1);
        countBitmap.add(2);

        assertEquals(Integer.valueOf(2), countBitmap.getCntBitmap().get(0).iterator().next());
        assertEquals(Integer.valueOf(1), countBitmap.getCntBitmap().get(1).iterator().next());
        assertEquals(2, countBitmap.getBitmap().getLongCardinality());
    }

    @Test
    public void testRemove() {
        CountBitmap countBitmap = new CountBitmap();
        countBitmap.add(1);
        countBitmap.add(1);
        countBitmap.add(2);

        boolean result1 = countBitmap.remove(2);
        assertTrue(result1);

        boolean result2 = countBitmap.remove(1);
        assertFalse(result2);

        boolean result3 = countBitmap.remove(1);
        assertTrue(result3);

        assertTrue(countBitmap.getBitmap().isEmpty());
    }
}
