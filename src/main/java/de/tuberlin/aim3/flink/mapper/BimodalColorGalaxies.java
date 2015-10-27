package de.tuberlin.aim3.flink.mapper;

import de.tuberlin.aim3.model.Galaxy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.math.BigInteger;

public class BimodalColorGalaxies implements FlatMapFunction<Tuple2<Galaxy, Integer>,Tuple3<Integer, Double,Double>> {

    @Override
    public void flatMap(Tuple2<Galaxy, Integer> value, Collector<Tuple3<Integer, Double, Double>> out) throws Exception {
        Double gr=0.0,ug=0.0;

        gr=(value.f0.getG()-value.f0.getR());
        ug=(value.f0.getU()-value.f0.getG());
        if((gr>(-0.2))&&(gr<1.5)&&(ug>0)&&(ug<2.2))
            out.collect(new Tuple3<Integer, Double, Double>(value.f1,gr,ug));

    }
}
