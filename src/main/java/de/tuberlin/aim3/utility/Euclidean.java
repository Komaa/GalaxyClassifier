package de.tuberlin.aim3.utility;


import java.util.Iterator;
import java.util.Map;

public class Euclidean {


    public static double calculateDistance(Map<String,Double> centroid, Map<String,Double> galaxy ){

        double distance = 0;

        Iterator keyIterator = centroid.entrySet().iterator();

        while ( keyIterator.hasNext()) {
            Map.Entry pairs = (Map.Entry)keyIterator.next();

            if(galaxy.containsKey(pairs.getKey())){

                distance = distance + Math.pow(((Double) (pairs.getValue())) - galaxy.get(pairs.getKey()), 2);
            }
        }

        return Math.sqrt(distance);

    }
}
