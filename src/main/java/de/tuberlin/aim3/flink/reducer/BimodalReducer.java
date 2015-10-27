package de.tuberlin.aim3.flink.reducer;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class BimodalReducer implements GroupReduceFunction<Tuple3<Integer, Double, Double>,Tuple3<Integer, Double, Double>> {

    @Override
    public void reduce(Iterable<Tuple3<Integer, Double, Double>> values, Collector<Tuple3<Integer, Double, Double>> out) throws Exception {

        Iterator<Tuple3<Integer, Double, Double>> iterator = values.iterator();

        while (iterator.hasNext()){

            out.collect(iterator.next());
        }
    }
}
