package de.tuberlin.aim3.flink.reducer;

import de.tuberlin.aim3.model.Galaxy;
import de.tuberlin.aim3.utility.Centroid;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class CentroidReducer implements GroupReduceFunction<Tuple2<Galaxy, Integer>, Tuple2<Galaxy, Integer>> {

    @Override
    public void reduce(Iterable<Tuple2<Galaxy, Integer>> values, Collector<Tuple2<Galaxy, Integer>> out) throws Exception {

        Iterator<Tuple2<Galaxy, Integer>> iterator = values.iterator();
        int counter = 0;

        Map<String,Double> centroidSum = new LinkedHashMap<String, Double>();
        Tuple2<Galaxy, Integer> tmpGalaxy = new Tuple2<Galaxy, Integer>();


        while (iterator.hasNext()){

              tmpGalaxy = iterator.next();
            Centroid.calculateCentroid(centroidSum,tmpGalaxy.f0.getFlux());
            counter++;
        }


        Centroid.calculateCentroidAverage(centroidSum,counter);
        Galaxy newCentroid = new Galaxy();
        newCentroid.setFlux(centroidSum);


        out.collect(new Tuple2<Galaxy, Integer>(newCentroid,tmpGalaxy.f1));

    }
}
