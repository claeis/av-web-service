package ch.ehi.av.webservice;

import ch.ehi.av.webservice.jaxb.geometry._1_0.*;

public class Jts2xtf24 {
    public MultiSurfaceType createMultiSurfaceType(com.vividsolutions.jts.geom.Geometry geometry) {
        if(geometry instanceof com.vividsolutions.jts.geom.Polygon) {
            SurfaceType surface=createSurfaceType((com.vividsolutions.jts.geom.Polygon)geometry);
            MultiSurfaceType ret=new MultiSurfaceType();
            ret.getSurface().add(new Surface(surface));
            return ret;
        }else if(geometry instanceof com.vividsolutions.jts.geom.MultiPolygon) {
            MultiSurfaceType ret=new MultiSurfaceType();
            com.vividsolutions.jts.geom.MultiPolygon jtsMulti=(com.vividsolutions.jts.geom.MultiPolygon)geometry;
            for(int i=0;i<jtsMulti.getNumGeometries();i++) {
                com.vividsolutions.jts.geom.Polygon jtsPoly=(com.vividsolutions.jts.geom.Polygon)jtsMulti.getGeometryN(i);
                SurfaceType surface=createSurfaceType(jtsPoly);
                ret.getSurface().add(new Surface(surface));
            }
            return ret;
        }
        throw new IllegalArgumentException("unexpected geometry type");
    }
    public SurfaceType createSurfaceType(com.vividsolutions.jts.geom.Geometry geometry) {
        if(geometry instanceof com.vividsolutions.jts.geom.Polygon) {
            com.vividsolutions.jts.geom.Polygon polygon=(com.vividsolutions.jts.geom.Polygon)geometry;
            com.vividsolutions.jts.geom.LineString jtsLineString=polygon.getExteriorRing();
            BoundaryType ring = createBoundaryType(jtsLineString);
            
            SurfaceType surface=new SurfaceType();
            surface.setExterior(ring);
            
            int ringc=polygon.getNumInteriorRing();
            for(int ringi=0;ringi<ringc;ringi++) {
                jtsLineString=polygon.getInteriorRingN(ringi);
                ring = createBoundaryType(jtsLineString);
                surface.getInterior().add(ring);
            }
            return surface;
        }
        throw new IllegalArgumentException("unexpected geometry type");
    }
    public Polyline createPolyline(com.vividsolutions.jts.geom.LineString jtsLine) {
        PolylineType curveProperty = createPolylineType(jtsLine);
        return new Polyline(curveProperty);
    }
    
    public PolylineType createPolylineType(com.vividsolutions.jts.geom.LineString jtsLineString) {
        PolylineType line=new PolylineType();
        com.vividsolutions.jts.geom.Coordinate jtsCoord[]=jtsLineString.getCoordinates();
        for(int i=0;i<jtsCoord.length;i++) {
            Coord pos = createCoord(jtsCoord[i]);
            if(i==0) {
                line.setCoord(pos);
            }else {
                line.getCoordOrArcOrCustomLineSegment().add(pos);
            }
        }
        return line;
    }
    public BoundaryType createBoundaryType(com.vividsolutions.jts.geom.LineString jtsLineString) {
        BoundaryType ring=new BoundaryType();
        Polyline polyline=createPolyline(jtsLineString);
        ring.setPolyline(polyline);
        return ring;
    }
    public Coord createCoord(com.vividsolutions.jts.geom.Coordinate jtsCoord) {
        CoordType directPos = createCoordType(jtsCoord);
        Coord pos = new Coord(directPos);
        return pos;
    }
    public CoordType createCoordType(com.vividsolutions.jts.geom.Coordinate jtsCoord) {
        CoordType pos=new CoordType();
        pos.setC1(jtsCoord.x);
        pos.setC2(jtsCoord.y);
        return pos;
    }
}
