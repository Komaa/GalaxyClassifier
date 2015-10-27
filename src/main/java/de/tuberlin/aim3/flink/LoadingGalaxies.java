package de.tuberlin.aim3.flink;

import de.tuberlin.aim3.flink.mapper.BimodalColorGalaxies;
import de.tuberlin.aim3.flink.mapper.CentroidLoader;
import de.tuberlin.aim3.flink.mapper.EuclideanMapper;
import de.tuberlin.aim3.flink.mapper.GalaxiesLoader;
import de.tuberlin.aim3.flink.reducer.BimodalReducer;
import de.tuberlin.aim3.flink.reducer.CentroidReducer;
import de.tuberlin.aim3.flink.reducer.GalaxiesClusterCounter;
import de.tuberlin.aim3.model.Galaxy;
import de.tuberlin.aim3.utility.Centroid;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.math.BigInteger;
import java.util.Iterator;


public class LoadingGalaxies {

    public static void main(String[] args) {

        String inputGalaxy, inputCentroid, output;

        if(args.length > 2) {

            inputGalaxy = args[0];
            inputCentroid = args[1];
            output = args[2];

            ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

            DataSource<String> inputGalaxies = environment.readTextFile(inputGalaxy);

            DataSource<String> inputCentroids = environment.readTextFile(inputCentroid);


            DataSet<Tuple2<Galaxy, Integer>> galaxies = inputGalaxies.flatMap(new GalaxiesLoader());

            DataSet<Tuple2<Galaxy, Integer>> preCentroids = inputCentroids.flatMap(new CentroidLoader());

            IterativeDataSet<Tuple2<Galaxy, Integer>> centroids = preCentroids.iterate(50);

            DataSet<Tuple2<Galaxy, Integer>> newCentroids = galaxies.flatMap(new EuclideanMapper()).withBroadcastSet(centroids, "centroids").
                    groupBy(1).reduceGroup(new CentroidReducer());


            DataSet<Tuple2<Galaxy, Integer>> finalCentroids = centroids.closeWith(newCentroids);


            DataSet<Tuple2<Galaxy, Integer>> clusteredGalaxies = galaxies.flatMap(new EuclideanMapper()).withBroadcastSet(finalCentroids, "centroids");

            DataSet<Tuple3<Integer, Double, Double>> bimodalColorGalaxies = clusteredGalaxies.flatMap(new BimodalColorGalaxies());

            DataSet<Tuple3<Integer, Double, Double>> bimodalColorGalaxiesPerEachCluster = bimodalColorGalaxies.groupBy(0).reduceGroup(new BimodalReducer());

            DataSet<Tuple2<Integer, Integer>> pointsInEachCluster = clusteredGalaxies.groupBy(1).reduceGroup(new GalaxiesClusterCounter());

            //bimodalColorGalaxies.print();


            finalCentroids.writeAsCsv(output+"ClusteringGalaxies/finalCentroids", FileSystem.WriteMode.OVERWRITE);
            clusteredGalaxies.writeAsCsv(output+"ClusteringGalaxies/clusteredGalaxies", FileSystem.WriteMode.OVERWRITE);
            bimodalColorGalaxies.writeAsCsv(output+"ClusteringGalaxies/bimodalColorGalaxies", FileSystem.WriteMode.OVERWRITE);
            bimodalColorGalaxiesPerEachCluster.writeAsCsv(output+"ClusteringGalaxies/bimodalColorGalaxiesPerEachCluster", FileSystem.WriteMode.OVERWRITE);
            pointsInEachCluster.writeAsCsv(output+"ClusteringGalaxies/pointsInEachCluster", FileSystem.WriteMode.OVERWRITE);


            try {

                environment.execute("GalaxiesClustering");
            } catch (Exception e) {

                System.out.println(e.getMessage());
            }
        }
        else
            throw new IllegalArgumentException("Please give InputGalaxy, InputCentroid and OutputPath as arguments");
    }
}
