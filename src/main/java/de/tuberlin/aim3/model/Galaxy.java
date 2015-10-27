package de.tuberlin.aim3.model;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;

public class Galaxy {

    private BigInteger galaxyId;
    private double flugG;
    private Map<String,Double> flux;
    private double r, g, u;

    public Galaxy(){
        flux = new LinkedHashMap<String, Double>();
    }

    public Map<String, Double> getFlux() {
        return flux;
    }

    public void setFlux(Map<String, Double> flux) {
        this.flux = flux;
    }

    public double getFlugG() {
        return flugG;
    }

    public void setFlugG(double flugG) {
        this.flugG = flugG;
    }

    public BigInteger getGalaxyId() {
        return galaxyId;
    }

    public void setGalaxyId(BigInteger galaxyId) {
        this.galaxyId = galaxyId;
    }

    public double getR() {
        return r;
    }

    public void setR(double r) {
        this.r = r;
    }

    public double getG() {
        return g;
    }

    public void setG(double g) {
        this.g = g;
    }

    public double getU() {
        return u;
    }

    public void setU(double u) {
        this.u = u;
    }
}
