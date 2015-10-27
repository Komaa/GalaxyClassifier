package de.tuberlin.aim3.utility;

import java.util.Iterator;
import java.util.Map;

public class Centroid {

        public static Map<String,Double> calculateCentroid(Map<String,Double> centroid, Map<String,Double> galaxy ) {

            Iterator keyIterator = galaxy.entrySet().iterator();

            while (keyIterator.hasNext()) {
                Map.Entry pairs = (Map.Entry) keyIterator.next();

                if (centroid.containsKey(pairs.getKey())) {

                    double tmpValue = centroid.get(pairs.getKey())+ galaxy.get(pairs.getKey());
                    centroid.put((String)pairs.getKey(), tmpValue);

                } else {

                    centroid.put((String) (pairs.getKey()), galaxy.get((String) (pairs.getKey())));
                }
            }

            return centroid;
        }


    public static Map<String,Double> calculateCentroidAverage(Map<String,Double> centroid, int count ) {

        Iterator keyIterator = centroid.entrySet().iterator();

        while (keyIterator.hasNext()) {

                Map.Entry pairs = (Map.Entry) keyIterator.next();
                double tmpValue = centroid.get(pairs.getKey()) / count;
                centroid.put((String)pairs.getKey(), tmpValue);

        }

        return centroid;
    }
}
