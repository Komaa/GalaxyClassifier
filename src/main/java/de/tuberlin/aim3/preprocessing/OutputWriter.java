
package de.tuberlin.aim3.preprocessing;

import java.io.*;
import java.util.ListIterator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.eso.fits.*;


public class OutputWriter {
    static FitsFile filetmp;
    static double redshift=0, spectroFlux_g;
    static String specObjID,plate, fiberID, mjd, u,g,r;
    static PolynomialSplineFunction interpolWave;

    public static void main(String[] argv) {

        //csv file input
        String csvFile = "/home/mustafa/Documents/GalaxyClassifier/src/main/resources/Mock.csv";
        BufferedWriter writermin=null, writergrater=null;
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        int countmiss=0,countall=0,countmin=0,countgrater=0, countphantom=0,countfind=0,countProcessing=0;

        //File of output if redshift<=0.1963

        String pathoutmin="/media/mustafa/Pie/Data_Mining_project/Result/inputgalaxymin.csv";
        File outputmin = new File(pathoutmin);

        try {
            outputmin.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            writermin = new BufferedWriter(new FileWriter(outputmin));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //File of output if redshift>0.1963
        File outputgreater = new File("/media/mustafa/Pie/Data_Mining_project/Result/inputgalaxygrater.csv");

        try {
            outputgreater.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            writergrater = new BufferedWriter(new FileWriter(outputgreater));
        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            br = new BufferedReader(new FileReader(csvFile));
            //read csv input until eof
            while ((line = br.readLine()) != null) {
                countall++;
                // use comma as separator
                String[] spectrumdata = line.split(cvsSplitBy);

//                System.out.println("specObjID= " + spectrumdata[0]
//                        + " , spectroFlux_g=" + spectrumdata[1] + " "
//                        + " , plate=" + spectrumdata[2] + " "
//                        + " , mjd=" + spectrumdata[3] + " "
//                        + " , fiberID=" + spectrumdata[4] + " "
//                        + " , redshift=" + spectrumdata[5] + " "
//                                + " , u=" + spectrumdata[6] + " "
//                                + " , g=" + spectrumdata[7] + " "
//                                + " , r=" + spectrumdata[8] + " ");

                redshift = Double.parseDouble(spectrumdata[5]);
                spectroFlux_g = Double.parseDouble(spectrumdata[1]);
                specObjID = spectrumdata[0];
                plate = spectrumdata[2];
                mjd = spectrumdata[3];
                fiberID = spectrumdata[4];
                u = spectrumdata[6];
                g = spectrumdata[7];
                r = spectrumdata[8];


                fiberID = StringUtils.leftPad(fiberID, 4, '0');
                plate = StringUtils.leftPad(plate, 4, '0');

                //Here we will compute the match between the object and the fits

                String fits = "/media/mustafa/Pie/Data_Mining_project/DATA/FITS/" + plate + "/spec-" + plate + "-" + mjd + "-" + fiberID + ".fits";
                //System.out.println(fits);


                File f = new File(fits);
                //check if file is present
                if (f.exists() && !f.isDirectory()) {
                    countfind++;

                    //interpolation of the wavelength if it is the first one
                    if (countfind == 1) {
                        interpolWave = new Interpolator().getInterval(fits);
                    }

                    //Output for control execution
                    if (countfind % 1000 == 0) {
                        countProcessing++;
                        System.out.println(countProcessing);
                    }


                    if (redshift <= 0.1963) {
                        writermin.write(specObjID + "\t" + spectroFlux_g + "\t" + u + "\t" + g + "\t" + r + "\t");
                    } else {
                        writergrater.write(specObjID + "\t" + spectroFlux_g + "\t" + u + "\t" + g + "\t" + r + "\t");
                    }


                    try {
                        filetmp = new FitsFile(fits);
                    } catch (FitsException e) {
                        System.out.println("Error: is not a FITS file >" + fits + "<");
                    } catch (IOException e) {
                        System.out.println("Error: cannot open file >" + fits + "<");
                    }

                    int noHDU = filetmp.getNoHDUnits();
                    for (int i = 0; i < noHDU; i++) {
                        FitsHDUnit hdu = filetmp.getHDUnit(i);
                        FitsHeader hdr = hdu.getHeader();
                        int type = hdr.getType();

                        if (type == Fits.BTABLE || type == Fits.ATABLE) {

                            FitsTable dm = (FitsTable) hdu.getData();

                            int nrow = dm.getNoRows();
                            int ncol = dm.getNoColumns();
                            FitsColumn col[] = new FitsColumn[ncol];

                            for (int n = 0; n < ncol; n++) {
                                col[n] = dm.getColumn(n);

                                if (col[n].getDataType() == 'F'
                                        || col[n].getDataType() == 'E'
                                        || col[n].getDataType() == 'D') {
                                    int npix = 0;
                                    double mean, rms, val;
                                    mean = rms = 0.0;

                                    for (int nr = 0; nr < nrow; nr++) {
                                        val = col[n].getReal(nr);
                                        if (Double.isNaN(val)) continue;
                                        npix++;
                                        mean += val;
                                        rms += val * val;
                                    }


                                } else if (col[n].getDataType() == 'I'
                                        || col[n].getDataType() == 'J'
                                        || col[n].getDataType() == 'B') {
                                    int npix = 0;
                                    double mean, rms, val;
                                    mean = rms = 0.0;

                                    for (int nr = 0; nr < nrow; nr++) {
                                        val = col[n].getInt(nr);
                                        if (val == Long.MIN_VALUE) continue;
                                        npix++;
                                        mean += val;
                                        rms += val * val;
                                    }

                                }


                            }

                            String fluxtmp = "";
                            double tmp;

                            for (int j = 0; j < nrow; j++) {

                                if (redshift <= 0.1963) {
                                    tmp = Math.pow(10, (col[1].getReal(j))) / (1 + redshift);
                                    tmp = Interpolator.samplewave(interpolWave, tmp);

                                } else {
                                    tmp = Math.pow(10, (col[1].getReal(j)));
                                }

                                //These are the mask explained in the paper
                                if (((tmp >= 4000) && (tmp <= 4420)) || ((tmp >= 4452) && (tmp <= 4474)) || ((tmp >= 4514) && (tmp <= 4559)) ||
                                        ((tmp >= 4634) && (tmp <= 4720)) || ((tmp >= 4800) && (tmp <= 5134)) || ((tmp >= 5154) && (tmp <= 5196)) ||
                                        ((tmp >= 5245) && (tmp <= 5285)) || ((tmp >= 5312) && (tmp <= 5352)) || ((tmp >= 5387) && (tmp <= 5415)) ||
                                        ((tmp >= 5696) && (tmp <= 5720)) || ((tmp >= 5776) && (tmp <= 5796)) || ((tmp >= 5876) && (tmp <= 5909)) ||
                                        ((tmp >= 5936) && (tmp <= 5994)) || ((tmp >= 6189) && (tmp <= 6272)) || ((tmp >= 6500) && (tmp <= 6800)) ||
                                        ((tmp >= 7000) && (tmp <= 7300)) || ((tmp >= 7500) && (tmp <= 7700))) {



                                    if (redshift <= 0.1963) {
                                         writermin.write(fluxtmp);
                                        fluxtmp = Interpolator.samplewave(interpolWave, (Math.pow(10, (col[1].getReal(j))) / (1 + redshift))) + ";" + col[0].getReal(j) + ",";
                                    } else {
                                        writergrater.write(fluxtmp);
                                        fluxtmp = Math.pow(10, (col[1].getReal(j))) + ";" + col[0].getReal(j) + ",";
                                    }

                                    // System.out.println(fluxtmp);

                                }
                            }
                            // System.out.println(fluxtmp.substring(0, fluxtmp.length() - 1));



                                if (!(fluxtmp.length() == 0)) {
                                    if (redshift <= 0.1963) {
                                        writermin.write(fluxtmp.substring(0, fluxtmp.length() - 1));
                                        writermin.newLine();
                                        countmin++;
                                    } else {
                                        writergrater.write(fluxtmp.substring(0, fluxtmp.length() - 1));
                                        writergrater.newLine();
                                        countgrater++;
                                    }
                                }
                        }
                    }
                    }else{
                        countmiss++;
                    }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            writermin.flush();
            writermin.close();
            writergrater.flush();
            writergrater.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Number element input csv: " + countall +" Number element matched with FITS: " + (countall-countmiss) + " Number element matched missed: " + countmiss);
        System.out.println("Number element with redshift<0.1963: " + countmin + " Number element with redshift>0.25: " + countgrater+" Number phantom element (all flux=0.0): " +countphantom);
        int ncentroid=17;
        RandomCentroidPicker.pickcentroid(pathoutmin,countmin,ncentroid);
        System.out.println("Create number "+ ncentroid + " centroids");
        System.exit(0);

    }
}
