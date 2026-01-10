package ch.ehi.av.webservice;

import com.vividsolutions.jts.geom.Geometry;

public class Grundstueck {
    private String nummer;
    private String nbident;
    private String egrid;
    private String art;
    private double flaechenmas;
    private Geometry geometrie;
    private int bfsNr;
    public String getNummer() {
        return nummer;
    }
    public void setNummer(String nummer) {
        this.nummer = nummer;
    }
    public String getNbident() {
        return nbident;
    }
    public void setNbident(String nbident) {
        this.nbident = nbident;
    }
    public String getEgrid() {
        return egrid;
    }
    public void setEgrid(String egrid) {
        this.egrid = egrid;
    }
    public String getArt() {
        return art;
    }
    public void setArt(String art) {
        this.art = art;
    }
    public double getFlaechenmas() {
        return flaechenmas;
    }
    public void setFlaechenmas(double flaechenmas) {
        this.flaechenmas = flaechenmas;
    }
    public Geometry getGeometrie() {
        return geometrie;
    }
    public void setGeometrie(Geometry geometrie) {
        this.geometrie = geometrie;
    }
}
