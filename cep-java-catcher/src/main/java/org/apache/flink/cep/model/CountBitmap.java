package org.apache.flink.cep.model;

import org.roaringbitmap.RoaringBitmap;

import java.util.HashMap;
import java.util.Map;

public class CountBitmap {

    private Map<Integer, RoaringBitmap> cntBitmap = new HashMap<>();

    private RoaringBitmap bitmap = new RoaringBitmap();

    public void add(int user) {
        int bit = 0;
        boolean loop = true;

        while (loop && bit < Integer.MAX_VALUE) {
            if (!cntBitmap.containsKey(bit)) cntBitmap.put(bit, new RoaringBitmap());

            if (cntBitmap.get(bit).contains(user)) {
                cntBitmap.get(bit).remove(user);
            } else {
                cntBitmap.get(bit).add(user);
                loop = false;
            }
            bit++;
        }

        bitmap.add(user);
    }

    /**
     *
     * @return true if this user is removed from bitmap.
     */
    public boolean remove(int user) {
        int bit = 0;
        boolean loop = true;
        boolean removeLast = false;

        if (!bitmap.contains(user)) return true;

        while (loop && bit < Integer.MAX_VALUE) {
            if (cntBitmap.get(bit).contains(user)) {
                cntBitmap.get(bit).remove(user);
                loop = false;
            } else {
                cntBitmap.get(bit).add(user);
            }
            bit++;
        }

        if (bit == 1) {
            while (true) {
                if (cntBitmap.containsKey(bit)) {
                    if (cntBitmap.get(bit).contains(user)) break;
                } else {
                    removeLast = true;
                    break;
                }
                bit++;
            }
        }

        if (removeLast) {
            bitmap.remove(user);
            return true;
        }

        return false;
    }

    public Map<Integer, RoaringBitmap> getCntBitmap() {
        return cntBitmap;
    }

    public RoaringBitmap getBitmap() {
        return bitmap;
    }
}
