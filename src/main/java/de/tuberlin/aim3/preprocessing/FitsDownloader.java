package de.tuberlin.aim3.preprocessing;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.concurrent.RunnableFuture;


public class FitsDownloader {

    double plate, fiberID, mjd;

    public static void main(String[] args) {

        //read csv file
        String csvFile = "./src/main/resources/Mock.csv";
        BufferedReader br = null;
        BufferedWriter bw = null;
        String line;

        try {

            br = new BufferedReader(new FileReader(csvFile));
            bw = new BufferedWriter(new FileWriter(new File("./src/main/resources/fitsFile.txt")));

            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] spectrumdata = line.split(",");


                String fitName = "http://data.sdss3.org/sas/dr12/boss/spectro/redux/v5_7_0/spectra/"+StringUtils.leftPad(spectrumdata[2],4,'0') + "/spec-" + StringUtils.leftPad(spectrumdata[2],4,'0') + "-" + spectrumdata[3] + "-" + StringUtils.leftPad(spectrumdata[4],4,'0')+".fits";
                bw.write(fitName);
                bw.newLine();
                System.out.println(fitName);


                fitName = "http://data.sdss3.org/sas/dr12/boss/spectro/redux/v5_7_2/spectra/"+StringUtils.leftPad(spectrumdata[2],4,'0') + "/spec-" + StringUtils.leftPad(spectrumdata[2],4,'0') + "-" + spectrumdata[3] + "-" + StringUtils.leftPad(spectrumdata[4],4,'0')+".fits";
                bw.write(fitName);
                bw.newLine();
                System.out.println(fitName);


                fitName = "http://data.sdss3.org/sas/dr12/sdss/spectro/redux/26/spectra/"+StringUtils.leftPad(spectrumdata[2],4,'0') + "/spec-" + StringUtils.leftPad(spectrumdata[2],4,'0') + "-" + spectrumdata[3] + "-" + StringUtils.leftPad(spectrumdata[4],4,'0')+".fits";
                bw.write(fitName);
                bw.newLine();
                System.out.println(fitName);

                fitName = "http://data.sdss3.org/sas/dr12/sdss/spectro/redux/103/spectra/"+StringUtils.leftPad(spectrumdata[2],4,'0') + "/spec-" + StringUtils.leftPad(spectrumdata[2],4,'0') + "-" + spectrumdata[3] + "-" + StringUtils.leftPad(spectrumdata[4],4,'0')+".fits";
                bw.write(fitName);
                bw.newLine();
                System.out.println(fitName);

                fitName = "http://data.sdss3.org/sas/dr12/sdss/spectro/redux/104/spectra/"+StringUtils.leftPad(spectrumdata[2],4,'0') + "/spec-" + StringUtils.leftPad(spectrumdata[2],4,'0') + "-" + spectrumdata[3] + "-" + StringUtils.leftPad(spectrumdata[4],4,'0')+".fits";
                bw.write(fitName);
                bw.newLine();
                System.out.println(fitName);
            }

            bw.flush();
            bw.close();
            br.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
