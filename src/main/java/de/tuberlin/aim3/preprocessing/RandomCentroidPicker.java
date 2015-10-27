package de.tuberlin.aim3.preprocessing;

import java.io.*;
import java.util.HashSet;
import java.util.Random;

public class RandomCentroidPicker {
    static BufferedWriter writer = null;
    static BufferedReader br = null;
    static String line = "";
    //File of output if redshift<=0.25

    public static void pickcentroid(String infile, int nrow,int ncentroid) {

        File outputcentroid = new File("/media/mustafa/Pie/Data_Mining_project/Result/initialcentroids.csv");
        HashSet<Integer> bancentroid=new HashSet<Integer>();

        try {
            outputcentroid.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            writer = new BufferedWriter(new FileWriter(outputcentroid));
        } catch (IOException e) {
            e.printStackTrace();
        }



        Random rand = new Random();

        for(int i=0;i<ncentroid;i++){
            int n;
            do {
                n = rand.nextInt(nrow) + 1;
            }while(bancentroid.contains(n));
            bancentroid.add(n);


            try {
                br = new BufferedReader(new FileReader(infile));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            for(int j=0;j<n;j++){
                try {
                    line = br.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            line=line+ "\t" + i;
            try {
                writer.write(line);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
