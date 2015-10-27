package de.tuberlin.aim3.flink.reducer;

import com.google.common.collect.Iterables;
import de.tuberlin.aim3.model.Galaxy;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.Iterator;


public class GalaxiesClusterCounter implements GroupReduceFunction<Tuple2<Galaxy, Integer>,Tuple2<Integer,Integer>> {

    @Override
    public void reduce(Iterable<Tuple2<Galaxy, Integer>> values, Collector<Tuple2<Integer, Integer>> out) throws Exception {

        Iterator iterator = values.iterator();

        Tuple2<Galaxy, Integer> tmp =(Tuple2<Galaxy, Integer>)iterator.next();

        int count = 1;

        while (iterator.hasNext()){

            iterator.next();
            count++;
        }


        out.collect(new Tuple2<Integer, Integer>(tmp.f1,count));

    }
}
