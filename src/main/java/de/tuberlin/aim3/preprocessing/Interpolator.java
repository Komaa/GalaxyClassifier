
package de.tuberlin.aim3.preprocessing;

import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.eso.fits.*;

import java.io.IOException;
import java.util.ArrayList;



public class Interpolator {
    static FitsFile filetmp;
    static double[] grid;
    static ArrayList<Double> gridlist= new ArrayList<Double>();
    static PolynomialSplineFunction interpola;


    public PolynomialSplineFunction getInterval(String fits){

        //read the fits file
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
            int noKw = hdr.getNoKeywords();
            int type = hdr.getType();
            int size = (int) hdr.getDataSize();


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
                        long time = System.currentTimeMillis();
                        for (int nr = 0; nr < nrow; nr++) {
                            val = col[n].getReal(nr);
                            if (Double.isNaN(val)) continue;
                            npix++;
                            mean += val;
                            rms += val * val;
                        }
                        float dtime =
                                (float) (1000.0 * (System.currentTimeMillis()
                                        - time) / ((double) nrow));
                        mean = mean / npix;
                        rms = rms / npix - mean * mean;
                        rms = ((0.0 < rms) ? Math.sqrt(rms) : 0.0);

                    } else if (col[n].getDataType() == 'I'
                            || col[n].getDataType() == 'J'
                            || col[n].getDataType() == 'B') {
                        int npix = 0;
                        double mean, rms, val;
                        mean = rms = 0.0;
                        long time = System.currentTimeMillis();
                        for (int nr = 0; nr < nrow; nr++) {
                            val = col[n].getInt(nr);
                            if (val == Long.MIN_VALUE) continue;
                            npix++;
                            mean += val;
                            rms += val * val;
                        }
                        float dtime =
                                (float) (1000.0 * (System.currentTimeMillis()
                                        - time) / ((double) nrow));
                        mean = mean / npix;
                        rms = rms / npix - mean * mean;
                        rms = ((0.0 < rms) ? Math.sqrt(rms) : 0.0);

                    }


                }
                double tmp;
                for (int j = 0; j < nrow; j++) {


                    tmp= Math.pow(10, (col[1].getReal(j)));
                    gridlist.add(tmp);
                }


            }

        }
        //function to crate an linear interpolation from an arraylist
        createinterpolation(gridlist);
        return interpola;
    }


    private PolynomialSplineFunction createinterpolation(ArrayList<Double> gridlist) {
        grid = new double[gridlist.size()];
        double[] tmp = new double[gridlist.size()];
        for (int i = 0; i < grid.length; i++) {
            grid[i] = gridlist.get(i);                // (outboxing)
        }

        for (int i = 0; i < grid.length; i++) {
            tmp[i] = i;                // (outboxing)
        }

        interpola=new LinearInterpolator().interpolate(grid,tmp);
        return interpola;
    }



    //function to return a sample of a wavelenght
    static public double samplewave(PolynomialSplineFunction inter, double wave){
        double sample=0.0;

        double[] samples=inter.getKnots();

        for(int i=0;i<samples.length;i++){
            if(samples[i]>=wave)
            {
                if(i!=samples.length-1) {
                    if (sample <= ((samples[i + 1] - samples[i]) / 2 + samples[i]))
                        sample = samples[i];
                    else
                        sample = samples[i + 1];
                }else
                    sample = samples[i];
                break;
            }

        }
        return sample;
    }
}
