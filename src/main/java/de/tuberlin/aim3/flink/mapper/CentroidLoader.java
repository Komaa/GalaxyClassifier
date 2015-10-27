package de.tuberlin.aim3.flink.mapper;

import de.tuberlin.aim3.model.Galaxy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


import java.math.BigInteger;


public class CentroidLoader  implements FlatMapFunction<String, Tuple2<Galaxy, Integer>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<Galaxy, Integer>> out) throws Exception {

        Galaxy galaxy = new Galaxy();
        String[] lineSplit = s.split("\t");
        galaxy.setGalaxyId(new BigInteger(lineSplit[0]));
        galaxy.setFlugG(Double.parseDouble(lineSplit[1]));

        int cluster = Integer.parseInt(lineSplit[6]);

        String[] fluxSplit = lineSplit[5].split(",");

        try {
            for (String str : fluxSplit) {
                String[] keyValueSplit = str.split(";");
                galaxy.getFlux().put(keyValueSplit[0], (Double.parseDouble(keyValueSplit[1])) / galaxy.getFlugG());
            }

            out.collect(new Tuple2<Galaxy, Integer>(galaxy, cluster));
        }catch (Exception e){
            System.out.println("CentroidLoader    "+ e.getMessage());
        }

    }
}
