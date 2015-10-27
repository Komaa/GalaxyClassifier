package de.tuberlin.aim3.flink.mapper;

import de.tuberlin.aim3.model.Galaxy;
import de.tuberlin.aim3.utility.Euclidean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.Collection;

public class EuclideanMapper extends RichFlatMapFunction<Tuple2<Galaxy, Integer>,Tuple2<Galaxy, Integer>> {

    private Collection<Tuple2<Galaxy,Integer>> centroids;

    @Override
    public void open(Configuration parameters) throws Exception {
        centroids = getRuntimeContext().getBroadcastVariable("centroids");

    }

    @Override
    public void flatMap(Tuple2<Galaxy, Integer> value, Collector<Tuple2<Galaxy, Integer>> out) throws Exception {

        double minDistance = Double.MAX_VALUE;
        int clusterId = -1;

        for(Tuple2<Galaxy,Integer> centroid : centroids){

            double distance = Euclidean.calculateDistance(centroid.f0.getFlux(), value.f0.getFlux());
            if(distance<minDistance){
                minDistance = distance;
                clusterId = centroid.f1;
            }
        }

        if(clusterId != -1) {
            out.collect(new Tuple2<Galaxy, Integer>(value.f0, clusterId));
        }

    }
}
