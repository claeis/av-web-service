package ch.ehi.av.webservice;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import jakarta.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import java.util.Base64;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.util.FileSystemUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ByteOrderValues;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import ch.ehi.av.webservice.jaxb.extract._1_0.GetCapabilitiesResponse;
import ch.ehi.av.webservice.jaxb.extract._1_0.GetCapabilitiesResponseType;
import ch.ehi.av.webservice.jaxb.extract._1_0.GetEGRIDResponse;
import ch.ehi.av.webservice.jaxb.extract._1_0.GetEGRIDResponseType;
import ch.ehi.av.webservice.jaxb.extract._1_0.GetExtractByIdResponse;
import ch.ehi.av.webservice.jaxb.extract._1_0.GetExtractByIdResponseType;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.CantonCode;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.Disclaimer;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.Extract;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.ExtractType;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LanguageCode;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LocalisedBlob;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LocalisedMText;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LocalisedText;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LocalisedUri;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.MultilingualBlob;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.MultilingualMText;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.MultilingualText;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.MultilingualUri;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.Office;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.PropertyType;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.PropertyTypeCode;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.RealEstateDPR;
import ch.ehi.av.webservice.jaxb.geometry._1_0.MultiSurfaceType;
import ch.ehi.av.webservice.jaxb.versioning._1_0.GetVersionsResponse;
import ch.ehi.av.webservice.jaxb.versioning._1_0.GetVersionsResponseType;
import ch.ehi.av.webservice.jaxb.versioning._1_0.VersionType;
// http://localhost:8080/extract/reduced/xml/geometry/CH693289470668


@Controller
public class AvController {
    
    private static final String LOCALISATION_V2_LOCALISEDBLOB = "localisation_v2_localisedblob";
	private static final String LOCALISATION_V2_MULTILINGUALBLOB = "localisation_v2_multilingualblob";
	private static final String AV_WBSRVC_V1_0KONFIGURATION_LOGO = "av_wbsrvc_v1_0konfiguration_logo";
	private static final String AV_WBSRVC_V1_0KONFIGURATION_INFORMATION = "av_wbsrvc_v1_0konfiguration_information";
	private static final String AV_WBSRVC_V1_0KONFIGURATION_ZUSTAENDIGESTELLE = "av_wbsrvc_v1_0konfiguration_zustaendigestelle";
	private static final String AV_WBSRVC_V1_0KONFIGURATION_AMT = "av_wbsrvc_v1_0konfiguration_amt";
	private static final String DMAVSP_CH_V1_0UNTERENHTGRNDBUCH_GRUNDBUCHKREIS = "dmavsp_ch_v1_0unterenhtgrndbuch_grundbuchkreis";
	private static final String AV_WBSRVC_V1_0KONFIGURATION_METADATENAV = "av_wbsrvc_v1_0konfiguration_metadatenav";
	private static final String DMADDR_STN = "offclndss_v2_2officlndxfddrsses_stn";
	private static final String DMADDR_ZIP = "offclndss_v2_2officlndxfddrsses_zip";
	private static final String DMADDR_ADDRESS = "offclndss_v2_2officlndxfddrsses_address";
	private static final String DMKONFIG_GRUNDSTUECKSARTTXT = "av_wbsrvc_v1_0konfiguration_grundstuecksarttxt";
	private static final String DMAV_BERGWERK = "dmav_grck_v1_0grundstuecke_bergwerk";
	private static final String DMAV_SELBSTRECHT = "dmav_grck_v1_0grundstuecke_selbstaendigesdauerndesrecht";
	private static final String DMAV_LIEGENSCHAFT = "dmav_grck_v1_0grundstuecke_liegenschaft";
	private static final String DMAV_GRUNDSTUECK = "dmav_grck_v1_0grundstuecke_grundstueck";
	private static final String DMAV_GEMEINDE = "dmav_hhnv_v1_0hoheitsgrenzenav_gemeinde";
	private static final String WMS_PARAM_WIDTH = "WIDTH";
    private static final String WMS_PARAM_HEIGHT = "HEIGHT";
    private static final String WMS_PARAM_DPI = "DPI";
    private static final String WMS_PARAM_BBOX = "BBOX";
    private static final String WMS_PARAM_SRS = "SRS";
    private static final String PARAM_CONST_PDF = "pdf";
    private static final String PARAM_CONST_XML = "xml";
    private static final String PARAM_CONST_URL = "url";
    private static final String PARAM_CONST_TRUE = "TRUE";
    private static final String PARAM_CONST_FALSE = "FALSE";
    private static final String PARAM_LOCALISATION = "LOCALISATION";
    private static final String PARAM_POSTALCODE = "POSTALCODE";
    private static final String PARAM_GNSS = "GNSS";
    private static final String PARAM_EN = "EN";
    private static final String PARAM_DPI = WMS_PARAM_DPI;
    private static final String PARAM_WITHIMAGES = "WITHIMAGES";
    private static final String PARAM_WITHOWNER = "WITHOWNER";
    private static final String PARAM_PROVISIONAL = "PROVISIONAL";
    private static final String PARAM_LANG = "LANG";
    private static final String PARAM_NUMBER = "NUMBER";
    private static final String PARAM_IDENTDN = "IDENTDN";
    private static final String PARAM_EGRID = "EGRID";
    private static final String PARAM_EGID = "EGID";
    private static final String PARAM_GEOMETRY = "GEOMETRY";

    private static final LanguageCode DE = LanguageCode.DE;
    private static final String LOGO_ENDPOINT = "logo";
    private static final String SYMBOL_ENDPOINT = "symbol";
    private static final String TMP_FOLDER_PREFIX = "avws";
    static final String SERVICE_SPEC_VERSION = "extract-1.0";
    private static final String FILE_EXT_XML = ".xml";
    
    private Logger logger=org.slf4j.LoggerFactory.getLogger(this.getClass());
    private Jts2xtf24 jts2xtf = new Jts2xtf24();
    
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    NamedParameterJdbcTemplate jdbcParamTemplate; 
    
    @Autowired
    Jaxb2Marshaller marshaller;
    
    //@Autowired
    //ch.so.agi.oereb.pdf4oereb.Converter extractXml2pdf;
    
    @Value("${spring.datasource.url}")
    private String dburl;
    @Value("${avws.dbschema}")
    private String dbschema;
    @Value("${avws.cadastreAuthorityUrl}")
    private String cadastreAuthorityUrl;
    @Value("${avws.webAppUrl}")
    private String webAppUrl;
    @Value("${avws.canton:Solothurn}")
    private String plrCanton;
    @Value("${avws.tmpdir:${java.io.tmpdir}}")
    private String oerebTmpdir;
    @Value("${avws.minIntersection:0.001}")
    private double minIntersection;
    @Value("${avws.dpi:300}")
    private int defaultMapDpi;
    
    @Value("${avws.planForLandregisterMainPage}")
    private String avPlanForLandregisterMainPage;
    @Value("${avws.planForLandregister}")
    private String avPlanForLandregister;
    @Value("${avws.planForSituation}")
    private String avPlanForSituation;
    
    @Value("${avws.subUnitOfLandRegisterDesignation}")
    private String subUnitOfLandRegisterDesignation;
    
    
    private static byte[] minimalImage=Base64.getDecoder().decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg==");

    @GetMapping("/")
    public ResponseEntity<String>  ping() {
        logger.info("env.dburl {}",dburl);
        return new ResponseEntity<String>("av web service",HttpStatus.OK);
    }
    @GetMapping("/logo/{id}")
    public ResponseEntity<byte[]>  getLogo(@PathVariable String id) {
        logger.info("id {}",id);
        byte image[]=getImageOrNull(id);
        if(image==null) {
            return new ResponseEntity<byte[]>(HttpStatus.NO_CONTENT);
        }
        return ResponseEntity
                .ok().header("content-disposition", "attachment; filename=" + id+".png")
                .contentLength(image.length)
                .contentType(MediaType.IMAGE_PNG).body(image);                
    }
    
    @GetMapping("/getegrid/")
    public ResponseEntity<GetEGRIDResponse>  getEgrid(@RequestParam Map<String, String> queryParameters) {
        String geometryParam=queryParameters.get(PARAM_GEOMETRY);
        boolean withGeometry=geometryParam!=null?PARAM_CONST_TRUE.equalsIgnoreCase(geometryParam):false;
        String identdn=queryParameters.get(PARAM_IDENTDN);
        String en=queryParameters.get(PARAM_EN);
        String gnss=queryParameters.get(PARAM_GNSS);
        String postalcode=queryParameters.get(PARAM_POSTALCODE);
        String localisation=queryParameters.get(PARAM_LOCALISATION);
        String number=queryParameters.get(PARAM_NUMBER);
        String egidParam=queryParameters.get(PARAM_EGID);
        if(identdn!=null) {
            return getEgridByNumber(withGeometry, identdn, number);
        }else if(en!=null || gnss!=null) {
            return getEgridByXY(withGeometry, en, gnss);
        }else if(egidParam!=null) {
            int egid=Integer.parseInt(egidParam);
            return getEgridByEgid(withGeometry, egid);
        }else if(postalcode!=null) {
            int plz=Integer.parseInt(postalcode);
            if(number!=null) {
                return getEgridByAddress(withGeometry, plz,localisation,number);
            }else {
                return getEgridByAddress(withGeometry, plz,localisation);
            }
        }
        throw new IllegalArgumentException("parameter IDENTDN or EN or GNSS or POSTALCODE or EGID expected");
    }
    ResponseEntity<GetEGRIDResponse>  getEgridByNumber(boolean withGeometry,String identdn,String number) {
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory of=new ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory();
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                "SELECT egrid,nummer,nbident,grundstuecksart as type FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" WHERE nummer=? AND nbident=?", new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapPropertyType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },number,identdn);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    ResponseEntity<GetEGRIDResponse>  getEgridByXY(boolean  withGeometry,String xy,String gnss) {
        if(xy==null && gnss==null) {
            throw new IllegalArgumentException("parameter EN or GNSS required");
        }else if(xy!=null && gnss!=null) {
            throw new IllegalArgumentException("only one of parameters EN or GNSS is allowed");
        }
        Coordinate coord = null;
        int srid = 2056;
        double scale = 1000.0;
        if(xy!=null) {
            coord=parseCoord(xy);
            srid = 2056;
            if(coord.x<2000000.0) {
                srid=21781;
            }
        }else {
            coord=parseCoord(gnss);
            srid = 4326;
            scale=100000.0;
        }
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN,true);
        PrecisionModel precisionModel=new PrecisionModel(scale);
        GeometryFactory geomFact=new GeometryFactory(precisionModel,srid);
        byte geom[]=geomEncoder.write(geomFact.createPoint(coord));
        // SELECT g.egris_egrid,g.nummer,g.nbident FROM oereb.dm01vch24lv95dliegenschaften_grundstueck g LEFT JOIN oereb.dm01vch24lv95dliegenschaften_liegenschaft l ON l.liegenschaft_von=g.t_id WHERE ST_DWithin(ST_GeomFromEWKT('SRID=2056;POINT( 2638242.500 1251450.000)'),l.geometrie,1.0)
        // SELECT g.egris_egrid,g.nummer,g.nbident FROM oereb.dm01vch24lv95dliegenschaften_grundstueck g LEFT JOIN oereb.dm01vch24lv95dliegenschaften_liegenschaft l ON l.liegenschaft_von=g.t_id WHERE ST_DWithin(ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT( 7.94554 47.41277)'),2056),l.geometrie,1.0)
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory of=new ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory();
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                "SELECT egrid,nummer,nbident,grundstuecksart as type FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" g"
                        +" LEFT JOIN (SELECT grundstueck as von, geometrie FROM "+getSchema()+"."+DMAV_LIEGENSCHAFT
                             +" UNION ALL SELECT grundstueck as von,  geometrie FROM "+getSchema()+"."+DMAV_SELBSTRECHT
                             +" UNION ALL SELECT grundstueck as von,     geometrie FROM "+getSchema()+"."+DMAV_BERGWERK+") b ON b.von=g.t_id WHERE ST_DWithin(ST_Transform(?,2056),b.geometrie,1.0)"
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapPropertyType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },geom);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    
    ResponseEntity<GetEGRIDResponse>  getEgridByAddress(boolean withGeometry, int postalcode,String localisation,String number) {
        logger.debug("postalcode {}",postalcode);
        logger.debug("localisation {}",localisation);
        logger.debug("number {}",number);
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory of=new ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory();
        String stmt="SELECT DISTINCT egrid,nummer,nbident,grundstuecksart as type FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" as g"
                +" JOIN ("
                + "(SELECT grundstueck as von, geometrie FROM "+getSchema()+"."+DMAV_LIEGENSCHAFT
                    +" UNION ALL SELECT grundstueck as von,  geometrie FROM "+getSchema()+"."+DMAV_SELBSTRECHT
                    +" UNION ALL SELECT grundstueck as von,     geometrie FROM "+getSchema()+"."+DMAV_BERGWERK+") as a "
                            + " JOIN (select adr.pnt_shape as lage from  "+getSchema()+"."+DMADDR_ADDRESS+" as adr " 
                            + " JOIN "+getSchema()+"."+DMADDR_ZIP+" AS zip ON adr.t_id=zip.offclndss_vrsss_ddress_zip_zip6  " 
                            + " JOIN "+getSchema()+"."+DMADDR_STN+" AS stn ON adr.t_id=stn.offclndss_vrsss_ddress_stn_name  " 
                            + " where zip.zip_zip4=? and stn.stn_text=? and adr.adr_number=? " 
                            + ") as ladr ON ST_Intersects(ladr.lage,a.geometrie)"
            + ") as b ON b.von=g.t_id";
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                stmt
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapPropertyType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },postalcode,localisation,number);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    ResponseEntity<GetEGRIDResponse>  getEgridByAddress(boolean withGeometry,int postalcode,String localisation) {
        logger.debug("postalcode {}",postalcode);
        logger.debug("localisation {}",localisation);
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory of=new ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory();
        String stmt="SELECT DISTINCT egrid,nummer,nbident,grundstuecksart as type FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" as g"
                +" JOIN ("
                + "(SELECT grundstueck as von, geometrie FROM "+getSchema()+"."+DMAV_LIEGENSCHAFT
                    +" UNION ALL SELECT grundstueck as von,  geometrie FROM "+getSchema()+"."+DMAV_SELBSTRECHT
                    +" UNION ALL SELECT grundstueck as von,     geometrie FROM "+getSchema()+"."+DMAV_BERGWERK+") as a "
                            + " JOIN (select adr.pnt_shape as lage from  "+getSchema()+"."+DMADDR_ADDRESS+" as adr " 
                            + " JOIN "+getSchema()+"."+DMADDR_ZIP+" AS zip ON adr.t_id=zip.offclndss_vrsss_ddress_zip_zip6  " 
                            + " JOIN "+getSchema()+"."+DMADDR_STN+" AS stn ON adr.t_id=stn.offclndss_vrsss_ddress_stn_name  " 
                            + " where zip.zip_zip4=? and stn.stn_text=? and adr.adr_number is null " 
                            + ") as ladr ON ST_Intersects(ladr.lage,a.geometrie)"
            + ") as b ON b.von=g.t_id";
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                stmt
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapPropertyType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },postalcode,localisation);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    ResponseEntity<GetEGRIDResponse>  getEgridByEgid(boolean withGeometry,int egid) {
        logger.debug("egid {}",egid);
        GetEGRIDResponseType ret= new GetEGRIDResponseType();
        ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory of=new ch.ehi.av.webservice.jaxb.extract._1_0.ObjectFactory();
        String stmt="SELECT DISTINCT egrid,nummer,nbident,grundstuecksart as type FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" as g"
                +" JOIN ("
                + "(SELECT grundstueck as von, geometrie FROM "+getSchema()+"."+DMAV_LIEGENSCHAFT
                    +" UNION ALL SELECT grundstueck as von,  geometrie FROM "+getSchema()+"."+DMAV_SELBSTRECHT
                    +" UNION ALL SELECT grundstueck as von,     geometrie FROM "+getSchema()+"."+DMAV_BERGWERK+") as a "
                            + " JOIN (select adr.pnt_shape as lage from  "+getSchema()+"."+DMADDR_ADDRESS+" as adr " 
                            + " where adr.bdg_egid=? " 
                            + ") as ladr ON ST_Intersects(ladr.lage,a.geometrie)"
            + ") as b ON b.von=g.t_id";
        List<JAXBElement<String>[]> gsList=jdbcTemplate.query(
                stmt
                , new RowMapper<JAXBElement<String>[]>() {
                    @Override
                    public JAXBElement<String>[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        JAXBElement ret[]=new JAXBElement[5];
                        String egrid=rs.getString(1);
                        ret[0]=of.createGetEGRIDResponseTypeEgrid(egrid);
                        ret[1]=of.createGetEGRIDResponseTypeNumber(rs.getString(2));
                        ret[2]=of.createGetEGRIDResponseTypeIdentDN(rs.getString(3));
                        ret[3]=of.createGetEGRIDResponseTypeType(mapPropertyType(rs.getString(4)));
                        if(withGeometry) {
                            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(getParcelGeometryByEgrid(egrid));
                            ret[4]=of.createGetEGRIDResponseTypeLimit(geomGml);
                        }
                        return ret;
                    }
                    
                },egid);
        for(JAXBElement[] gs:gsList) {
            ret.getEgridAndNumberAndIdentDN().add(gs[0]);
            ret.getEgridAndNumberAndIdentDN().add(gs[1]);
            ret.getEgridAndNumberAndIdentDN().add(gs[2]);
            ret.getEgridAndNumberAndIdentDN().add(gs[3]);
            if(withGeometry) {
                ret.getEgridAndNumberAndIdentDN().add(gs[4]);
            }
        }
         return new ResponseEntity<GetEGRIDResponse>(new GetEGRIDResponse(ret),gsList.size()>0?HttpStatus.OK:HttpStatus.NO_CONTENT);
    }
    
    @GetMapping(value="/extract/{format}/",consumes=MediaType.ALL_VALUE,produces = {MediaType.APPLICATION_PDF_VALUE,MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?>  getExtract(@PathVariable String format,@RequestParam Map<String, String> queryParameters) {
        String geometryParam=queryParameters.get(PARAM_GEOMETRY);
        boolean withGeometry=geometryParam!=null?PARAM_CONST_TRUE.equalsIgnoreCase(geometryParam):false;
        String egrid=queryParameters.get(PARAM_EGRID);
        String identdn=queryParameters.get(PARAM_IDENTDN);
        String number=queryParameters.get(PARAM_NUMBER);
        String lang=queryParameters.get(PARAM_LANG);
        String withImagesParam=queryParameters.get(PARAM_WITHIMAGES);
        boolean withImages = withImagesParam==null?false:PARAM_CONST_TRUE.equalsIgnoreCase(withImagesParam);
        String withOwnerParam=queryParameters.get(PARAM_WITHOWNER);
        boolean withOwner = withOwnerParam==null?true:PARAM_CONST_FALSE.equalsIgnoreCase(withOwnerParam);
        String provisionalParam=queryParameters.get(PARAM_PROVISIONAL);
        boolean provisional = provisionalParam==null?false:PARAM_CONST_TRUE.equalsIgnoreCase(provisionalParam);
        String dpiParam=queryParameters.get(PARAM_DPI);
        int dpi=dpiParam!=null?Integer.parseInt(dpiParam):defaultMapDpi;
        if(format.equalsIgnoreCase(PARAM_CONST_URL)) {
            return getExtractRedirect(egrid,identdn,number);
        }else if(egrid!=null) {
            if(withGeometry) {
                return getExtractWithGeometryByEgrid(format,egrid,lang,provisional,withOwner,withImages,dpi);
            }
            return getExtractWithoutGeometryByEgrid(format,egrid,lang,provisional,withOwner,withImages,dpi);
        }else {
            if(withGeometry) {
                return getExtractWithGeometryByNumber(format,identdn,number,lang,provisional,withOwner,withImages,dpi);
            }
            return getExtractWithoutGeometryByNumber(format,identdn,number,lang,provisional,withOwner,withImages,dpi);
        }
    }
                
    ResponseEntity<?>  getExtractWithGeometryByEgrid(String format,String egrid,String lang,boolean provisional,boolean withOwner,boolean withImages,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByEgrid(egrid);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getNbident());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        boolean withGeometry = true;
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_XML);
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,responseHeaders,HttpStatus.OK);
    }
    ResponseEntity<?>  getExtractWithoutGeometryByEgrid(String format,String egrid,String lang,boolean provisional,boolean withOwner,boolean withImages,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByEgrid(egrid);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getNbident());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }

        boolean withGeometry = false;
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_XML);
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,responseHeaders,HttpStatus.OK);
    }    
    private ResponseEntity<?>  getExtractWithGeometryByNumber(String format,String identdn,String number,String lang,boolean provisional,boolean withOwner,boolean withImages,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByNumber(identdn,number);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getNbident());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }

        boolean withGeometry = true;
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_XML);
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,responseHeaders,HttpStatus.OK);
    }    
    private ResponseEntity<?> getExtractRedirect(String egridParam, String identdn, String number) {
        String egrid=verifyEgrid(egridParam, identdn, number);
        if(egrid==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        HttpHeaders headers=new HttpHeaders();
        headers.add(HttpHeaders.LOCATION, getWebAppUrl(egrid));
        ResponseEntity<Object> ret=new ResponseEntity<Object>(headers,HttpStatus.SEE_OTHER);
        return ret;
    }

    private ResponseEntity<?>  getExtractWithoutGeometryByNumber(String format,String identdn,String number,String lang,boolean provisional,boolean withOwner,boolean withImages,int dpi) {
        if(!format.equals(PARAM_CONST_XML) && !format.equals(PARAM_CONST_PDF)) {
            throw new IllegalArgumentException("unsupported format <"+format+">");
        }
        Grundstueck parcel=getParcelByNumber(identdn,number);
        if(parcel==null) {
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }
        java.sql.Date basedataDate=getBasedatadateOfMunicipality(parcel.getNbident());
        if(basedataDate==null) {
            // non unlocked municipality
            return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
        }

        boolean withGeometry = false;
        if(format.equals(PARAM_CONST_PDF)) {
            withImages = true;
            withGeometry = true;
        }
        Extract extract=createExtract(parcel.getEgrid(),parcel,basedataDate,withGeometry,withImages,dpi);
        
        GetExtractByIdResponseType response=new GetExtractByIdResponseType();
        response.setExtract(extract);
        GetExtractByIdResponse responseEle=new GetExtractByIdResponse(response);
        
        if(format.equals(PARAM_CONST_PDF)) {
            return createExtractAsPdf(parcel, responseEle);
        }
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_XML);
        return new ResponseEntity<GetExtractByIdResponse>(responseEle,responseHeaders,HttpStatus.OK);
    }    
    @GetMapping("/capabilities")
    public @ResponseBody  GetCapabilitiesResponse getCapabilities() {
        GetCapabilitiesResponseType ret=new GetCapabilitiesResponseType();
        
        // Liste der vorhandenen Gemeinden;
        List<Integer> gemeinden=jdbcTemplate.query(
                "SELECT bfsnummer FROM "+getSchema()+"."+DMAV_GEMEINDE, new RowMapper<Integer>() {
                    @Override
                    public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return rs.getInt(1);
                    }
                    
                });
        ret.getMunicipality().addAll(gemeinden);
        // Liste der unterstuetzten Sprachen (2 stellige ISO Codes);
        ret.getLanguage().add("de");
        // Liste der unterstuetzten CRS.
        ret.getCrs().add("2056");
        return new GetCapabilitiesResponse(ret);
    }

    @GetMapping("/versions")
    public @ResponseBody  GetVersionsResponse getVersions() {
        GetVersionsResponseType ret=new GetVersionsResponseType();
        VersionType ver=new VersionType();
        ver.setVersion(SERVICE_SPEC_VERSION);
        ret.getSupportedVersion().add(ver);
        return new GetVersionsResponse(ret);
    }
    
    private ResponseEntity<?> createExtractAsPdf(Grundstueck parcel, GetExtractByIdResponse responseEle) {
        java.io.File tmpFolder=new java.io.File(oerebTmpdir,TMP_FOLDER_PREFIX+Thread.currentThread().getId());
        if(!tmpFolder.exists()) {
            tmpFolder.mkdirs();
        }
        logger.info("tmpFolder {}",tmpFolder.getAbsolutePath());
        java.io.File tmpExtractFile=new java.io.File(tmpFolder,parcel.getEgrid()+FILE_EXT_XML);
        marshaller.marshal(responseEle,new javax.xml.transform.stream.StreamResult(tmpExtractFile));
        throw new IllegalStateException();
/*        try {
            java.io.File pdfFile=extractXml2pdf.runXml2Pdf(tmpExtractFile.getAbsolutePath(), tmpFolder.getAbsolutePath(), Locale.DE);
            pdfFile.getName();
            java.io.InputStream is = new java.io.FileInputStream(pdfFile);
            return ResponseEntity
                    .ok().header("content-disposition", "attachment; filename=" + pdfFile.getName())
                    .contentLength(pdfFile.length())
                    .contentType(MediaType.APPLICATION_PDF).body(new InputStreamResource(is));                
        } catch (ConverterException e) {
            throw new IllegalStateException(e);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        } */
    }    
    private Extract createExtract(String egrid, Grundstueck parcel, java.sql.Date basedataDate,boolean withGeometry, boolean withImages,int dpi) {
        ExtractType extract=new ExtractType();
        logger.info("timezone id {}",TimeZone.getDefault().getID());
        XMLGregorianCalendar today=createXmlDate(new java.util.Date());
        extract.setCreationDate(today);
        extract.setExtractIdentifier(UUID.randomUUID().toString());
        // Grundstueck
        final Geometry parcelGeom = parcel.getGeometrie();
        Envelope bbox = getMapBBOX(parcelGeom);
        setParcel(extract,egrid,parcel,bbox,withGeometry,withImages,dpi);

        // Logos
        if(withImages) {
            extract.setPropertyInformationLogo(getImage("ch.pi"));
            extract.setFederalLogo(getImage("ch"));
            extract.setCantonalLogo(getImage("ch."+extract.getRealEstateDPR().getCanton().name()));
            extract.setMunicipalityLogo(getImage("ch."+extract.getRealEstateDPR().getMunicipalityCode()));
        }else {
            extract.setPropertyInformationLogoRef(getLogoRef("ch.pi"));
            extract.setFederalLogoRef(getLogoRef("ch"));
            extract.setCantonalLogoRef(getLogoRef("ch."+extract.getRealEstateDPR().getCanton().name()));
            extract.setMunicipalityLogoRef(getLogoRef("ch."+extract.getRealEstateDPR().getMunicipalityCode()));
        }
        // Text

        setBaseData(extract,basedataDate);
        
        extract.setDisclaimer(getDisclaimer("ch.pi"));
        {
            // Katasteramt
            Office piAuthority = new Office();
            piAuthority.setName(createMultilingualTextType("Katasteramt"));
            piAuthority.setOfficeAtWeb(createMultilingualUri(cadastreAuthorityUrl));

            setOffice(piAuthority);
            extract.setPropertyInformationAuthority(piAuthority);
        }
        
        return new Extract(extract);
    }

    private XMLGregorianCalendar createXmlDate(Date date) {
        try {
            GregorianCalendar gdate=new GregorianCalendar();
            gdate.setTime(date);
            XMLGregorianCalendar today = DatatypeFactory.newInstance().newXMLGregorianCalendar(gdate);
            return today;
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }
    private MultilingualUri createMultilingualUri(String ref) {
        if(ref==null || ref.length()==0) {
            return null;
        }
        LocalisedUri uri=new LocalisedUri();
        //uri.setLanguage(value);
        uri.setText(ref);
        MultilingualUri ret=new MultilingualUri();
        ret.getLocalisedText().add(uri);
        return ret;
    }
    
    private String getWebAppUrl(String egrid) {
        return webAppUrl+egrid;
    }
    private String getSymbolRef(String id) {
        return ServletUriComponentsBuilder.fromCurrentContextPath().pathSegment(SYMBOL_ENDPOINT).pathSegment(id).build().toUriString();
    }
    private String getLogoRef(String id) {
        return ServletUriComponentsBuilder.fromCurrentContextPath().pathSegment(LOGO_ENDPOINT).pathSegment(id).build().toUriString();
    }
    private byte[] getImage(String code) {
        byte[] ret=getImageOrNull(code);
        if(ret!=null) {
            return ret;
        }
        return minimalImage;
    }
    private byte[] getImageOrNull(String code) {
        java.util.List<Map<String,Object>> baseData=jdbcTemplate.queryForList(
                "SELECT b.content as logo,b_de.content as logo_de FROM "+getSchema()+"."+AV_WBSRVC_V1_0KONFIGURATION_LOGO+" AS l"
                +" LEFT JOIN "+getSchema()+"."+LOCALISATION_V2_MULTILINGUALBLOB+" AS mb ON mb.av_wbsrvc_vnfgrtn_logo_bild=l.t_id"
                +" LEFT JOIN (SELECT content,loclstn_v2_mltlnglblob_localisedblob FROM "+getSchema()+"."+LOCALISATION_V2_LOCALISEDBLOB+" WHERE alanguage IS NULL) AS b ON b.loclstn_v2_mltlnglblob_localisedblob=mb.t_id"
                +" LEFT JOIN "+getSchema()+"."+LOCALISATION_V2_MULTILINGUALBLOB+" AS mb_de ON mb_de.av_wbsrvc_vnfgrtn_logo_bild=l.t_id"
                +" LEFT JOIN (SELECT content,loclstn_v2_mltlnglblob_localisedblob FROM "+getSchema()+"."+LOCALISATION_V2_LOCALISEDBLOB+" WHERE alanguage='de') AS b_de ON b_de.loclstn_v2_mltlnglblob_localisedblob=mb_de.t_id"
                +" WHERE l.acode=?",code);
        if(baseData!=null && baseData.size()==1) {
            byte[] logo=(byte[])baseData.get(0).get("logo");
            byte[] logo_de=(byte[])baseData.get(0).get("logo_de");
            if(logo_de!=null) {
                return logo_de;
            }
            return logo;
        }
        return null;
    }

    private void setOffice(Office office) {
        java.util.Map<String,Object> baseData=null;
        try {
            String sqlStmt=
                    "SELECT aname,aname_de,amtimweb,amtimweb_de, auid,zeile1,zeile2,strasse,hausnr,plz,ort FROM "+getSchema()+"."+AV_WBSRVC_V1_0KONFIGURATION_AMT+" AS ea"
                            +" WHERE amtimweb=? OR amtimweb_de=?";
            logger.info("stmt {} ",sqlStmt);
            String uri=getUri(office.getOfficeAtWeb());
            baseData=jdbcTemplate.queryForMap(sqlStmt
            ,uri,uri);
        }catch(EmptyResultDataAccessException ex) {
            ; // ignore if no record found
        }
        if(baseData!=null) {
            office.setName(createMultilingualTextType(baseData, "aname"));
            office.setOfficeAtWeb(createMultilingualUri(baseData, "amtimweb"));
            office.setUID((String) baseData.get("auid"));
            office.setLine1((String) baseData.get("zeile2"));
            office.setLine2((String) baseData.get("zeile1"));
            office.setStreet((String) baseData.get("strasse"));
            office.setNumber((String) baseData.get("hausnr"));
            office.setPostalCode((String) baseData.get("plz"));
            office.setCity((String) baseData.get("ort"));
        }
    }

    private String getUri(MultilingualUri multilingualUri) {
        if(multilingualUri==null) {
            return null;
        }
        for(LocalisedUri uri:multilingualUri.getLocalisedText()) {
            return uri.getText();
        }
        return null;
    }

    private Disclaimer getDisclaimer(String section) {
        java.util.List<java.util.Map<String,Object>> baseDataList=jdbcTemplate.queryForList(
                "SELECT inhalt_de,inhalt_fr,inhalt_it,inhalt_rm,inhalt_en FROM "+getSchema()+"."+AV_WBSRVC_V1_0KONFIGURATION_INFORMATION+" WHERE acode=?",section);
        for(java.util.Map<String,Object> baseData:baseDataList) {
            MultilingualText content = createMultilingualTextType(baseData,"inhalt");
            Disclaimer exclOfLiab=new Disclaimer();
            exclOfLiab.setContent(content);
            return exclOfLiab;
        }
        return null;
    }

    private void setBaseData(ExtractType extract,java.sql.Date basedataDate) {
        XMLGregorianCalendar basedataXml=createXmlDate(basedataDate);
        extract.setUpdateDateCS(basedataXml);
    }

    private MultilingualMText createMultilingualMTextType(Map<String, Object> baseData,String prefix) {
        MultilingualMText ret=new MultilingualMText();
        for(LanguageCode lang:LanguageCode.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null && txt.length()>0) {
                LocalisedMText lTxt= new LocalisedMText();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualMText createMultilingualMTextType(Map<String, Object> baseData,String prefix,Map<String,String> params) {
        MultilingualMText ret=new MultilingualMText();
        for(LanguageCode lang:LanguageCode.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null && txt.length()>0) {
                if(params!=null) {
                    for(String key:params.keySet()) {
                        String param="${"+key+"}";
                        int pos=txt.indexOf(param);
                        if(pos>-1) {
                            txt=txt.substring(0, pos)+params.get(key)+txt.substring(pos+param.length());
                        }
                    }
                }
                LocalisedMText lTxt= new LocalisedMText();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualText createMultilingualTextType(Map<String, Object> baseData,String prefix) {
        MultilingualText ret=new MultilingualText();
        {
            String txt=(String)baseData.get(prefix);
            if(txt!=null) {
                LocalisedText lTxt= new LocalisedText();
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        for(LanguageCode lang:LanguageCode.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null) {
                LocalisedText lTxt= new LocalisedText();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    
    private String getMultilingualUri(MultilingualUri multilingualUri) {
        return multilingualUri.getLocalisedText().get(0).getText();
    }
    
    private MultilingualUri createMultilingualUri(Map<String, Object> baseData,String prefix) {
        MultilingualUri ret=new MultilingualUri();
        {
            String txt=(String)baseData.get(prefix);
            if(txt!=null) {
                LocalisedUri lTxt= new LocalisedUri();
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        for(LanguageCode lang:LanguageCode.values()) {
            String txt=(String)baseData.get(prefix+"_"+lang.value());
            if(txt!=null) {
                LocalisedUri lTxt= new LocalisedUri();
                lTxt.setLanguage(lang);
                lTxt.setText(txt);
                ret.getLocalisedText().add(lTxt);
            }
        }
        return ret;
    }
    private MultilingualMText createMultilingualMTextType(String txt) {
        LocalisedMText lTxt = createLocalizedMText(txt);
        if(lTxt==null) {
            return null;
        }
        MultilingualMText ret=new MultilingualMText();
        ret.getLocalisedText().add(lTxt);
        return ret;
    }
    private MultilingualText createMultilingualTextType(String txt) {
        LocalisedText lTxt = createLocalizedText(txt);
        if(lTxt==null) {
            return null;
        }
        MultilingualText ret=new MultilingualText();
        ret.getLocalisedText().add(lTxt);
        return ret;
    }
    private MultilingualUri createMultilingualUri_de(String txt) {
        LocalisedUri lTxt = createLocalizedUri_de(txt);
        if(lTxt==null) {
            return null;
        }
        MultilingualUri ret=new MultilingualUri();
        ret.getLocalisedText().add(lTxt);
        return ret;
    }

    private LocalisedMText createLocalizedMText(String txt) {
        if(txt==null || txt.length()==0) {
            return null;
        }
        LocalisedMText lTxt= new LocalisedMText();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private LocalisedText createLocalizedText(String txt) {
        if(txt==null || txt.length()==0) {
            return null;
        }
        LocalisedText lTxt= new LocalisedText();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }
    private LocalisedUri createLocalizedUri_de(String txt) {
        if(txt==null || txt.length()==0) {
            return null;
        }
        LocalisedUri lTxt= new LocalisedUri();
        lTxt.setLanguage(DE);
        lTxt.setText(txt);
        return lTxt;
    }

    private String getSchema() {
        return dbschema!=null?dbschema:"xav";
    }
    
    private Coordinate parseCoord(String xy) {
        int sepPos=xy.indexOf(',');
        double x=Double.parseDouble(xy.substring(0, sepPos));
        double y=Double.parseDouble(xy.substring(sepPos+1));
        Coordinate coord=new Coordinate(x,y);
        return coord;
    }
    private Grundstueck getParcelByEgrid(String egrid) {
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        List<Grundstueck> gslist=jdbcTemplate.query(
                "SELECT ST_AsBinary(l.geometrie) as l_geometrie,ST_AsBinary(s.geometrie) as s_geometrie,ST_AsBinary(b.geometrie) as b_geometrie,nummer,nbident,grundstuecksart,gesamtflaechenmass,l.flaechenmass as l_flaechenmass,s.flaechenmass as s_flaechenmass,b.flaechenmass as b_flaechenmass FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" g"
                        +" LEFT JOIN "+getSchema()+"."+DMAV_LIEGENSCHAFT+" l ON g.t_id=l.grundstueck "
                        +" LEFT JOIN "+getSchema()+"."+DMAV_SELBSTRECHT+" s ON g.t_id=s.grundstueck"
                        +" LEFT JOIN "+getSchema()+"."+DMAV_BERGWERK+" b ON g.t_id=b.grundstueck"
                        +" WHERE g.egrid=?", new RowMapper<Grundstueck>() {
                    WKBReader decoder=new WKBReader(geomFactory);
                    
                    @Override
                    public Grundstueck mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Geometry polygon=null;
                        byte l_geometrie[]=rs.getBytes("l_geometrie");
                        byte s_geometrie[]=rs.getBytes("s_geometrie");
                        byte b_geometrie[]=rs.getBytes("b_geometrie");
                        try {
                            if(l_geometrie!=null) {
                                polygon=decoder.read(l_geometrie);
                            }else if(s_geometrie!=null) {
                                polygon=decoder.read(s_geometrie);
                            }else if(b_geometrie!=null) {
                                polygon=decoder.read(b_geometrie);
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                            if(polygon==null || polygon.isEmpty()) {
                                return null;
                            }
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        Grundstueck ret=new Grundstueck();
                        ret.setGeometrie(polygon);
                        ret.setEgrid(egrid);
                        ret.setNummer(rs.getString("nummer"));
                        ret.setNbident(rs.getString("nbident"));
                        ret.setArt(rs.getString("grundstuecksart"));
                        int f=rs.getInt("gesamtflaechenmass");
                        if(rs.wasNull()) {
                            if(l_geometrie!=null) {
                                f=rs.getInt("l_flaechenmass");
                            }else if(s_geometrie!=null) {
                                f=rs.getInt("s_flaechenmass");
                            }else if(b_geometrie!=null) {
                                f=rs.getInt("b_flaechenmass");
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                        }
                        ret.setFlaechenmas(f);
                        return ret;
                    }

                    
                },egrid);
        if(gslist==null || gslist.isEmpty()) {
            return null;
        }
        Polygon polygons[]=new Polygon[gslist.size()];
        int i=0;
        for(Grundstueck gs:gslist) {
            polygons[i++]=(Polygon)gs.getGeometrie();
        }
        Geometry multiPolygon=geomFactory.createMultiPolygon(polygons);
        Grundstueck gs=gslist.get(0);
        gs.setGeometrie(multiPolygon);
                
        return gs;
    }
    private Grundstueck getParcelByNumber(String nbident,String nr) {
        PrecisionModel precisionModel=new PrecisionModel(1000.0);
        GeometryFactory geomFactory=new GeometryFactory(precisionModel);
        List<Grundstueck> gslist=jdbcTemplate.query(
                "SELECT"
                + " ST_AsBinary(l.geometrie) as l_geometrie"
                + ",ST_AsBinary(s.geometrie) as s_geometrie"
                + ",ST_AsBinary(b.geometrie) as b_geometrie"
                + ",egrid"
                + ",grundstuecksart"
                + ",gesamtflaechenmass"
                + ",l.flaechenmass as l_flaechenmass"
                + ",s.flaechenmass as s_flaechenmass"
                + ",b.flaechenmass as b_flaechenmass"
                + " FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" g"
                        +" LEFT JOIN "+getSchema()+"."+DMAV_LIEGENSCHAFT+" l ON g.t_id=l.grundstueck "
                        +" LEFT JOIN "+getSchema()+"."+DMAV_SELBSTRECHT+" s ON g.t_id=s.grundstueck"
                        +" LEFT JOIN "+getSchema()+"."+DMAV_BERGWERK+" b ON g.t_id=b.grundstueck"
                        +" WHERE g.nbident=? AND g.nummer=?", new RowMapper<Grundstueck>() {
                    WKBReader decoder=new WKBReader(geomFactory);
                    
                    @Override
                    public Grundstueck mapRow(ResultSet rs, int rowNum) throws SQLException {
                        Geometry polygon=null;
                        byte l_geometrie[]=rs.getBytes("l_geometrie");
                        byte s_geometrie[]=rs.getBytes("s_geometrie");
                        byte b_geometrie[]=rs.getBytes("b_geometrie");
                        try {
                            if(l_geometrie!=null) {
                                polygon=decoder.read(l_geometrie);
                            }else if(s_geometrie!=null) {
                                polygon=decoder.read(s_geometrie);
                            }else if(b_geometrie!=null) {
                                polygon=decoder.read(b_geometrie);
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                            if(polygon==null || polygon.isEmpty()) {
                                return null;
                            }
                        } catch (ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        Grundstueck ret=new Grundstueck();
                        ret.setGeometrie(polygon);
                        ret.setEgrid(rs.getString("egrid"));
                        ret.setNummer(nr);
                        ret.setNbident(nbident);
                        ret.setArt(rs.getString("grundstuecksart"));
                        int f=rs.getInt("gesamtflaechenmass");
                        if(rs.wasNull()) {
                            if(l_geometrie!=null) {
                                f=rs.getInt("l_flaechenmass");
                            }else if(s_geometrie!=null) {
                                f=rs.getInt("s_flaechenmass");
                            }else if(b_geometrie!=null) {
                                f=rs.getInt("b_flaechenmass");
                            }else {
                                throw new IllegalStateException("no geometrie");
                            }
                        }
                        ret.setFlaechenmas(f);
                        return ret;
                    }

                    
                },nbident,nr);
        if(gslist==null || gslist.isEmpty()) {
            return null;
        }
        Polygon polygons[]=new Polygon[gslist.size()];
        int i=0;
        for(Grundstueck gs:gslist) {
            polygons[i++]=(Polygon)gs.getGeometrie();
        }
        Geometry multiPolygon=geomFactory.createMultiPolygon(polygons);
        Grundstueck gs=gslist.get(0);
        gs.setGeometrie(multiPolygon);
        
        return gs;
    }
    private Geometry getParcelGeometryByEgrid(String egrid) {
            PrecisionModel precisionModel=new PrecisionModel(1000.0);
            GeometryFactory geomFactory=new GeometryFactory(precisionModel);
            List<Geometry> gslist=jdbcTemplate.query(
                    "SELECT ST_AsBinary(l.geometrie) as l_geometrie,ST_AsBinary(s.geometrie) as s_geometrie,ST_AsBinary(b.geometrie) as b_geometrie FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" g"
                            +" LEFT JOIN "+getSchema()+"."+DMAV_LIEGENSCHAFT+" l ON g.t_id=l.grundstueck"
                            +" LEFT JOIN "+getSchema()+"."+DMAV_SELBSTRECHT+" s ON g.t_id=s.grundstueck"
                            +" LEFT JOIN "+getSchema()+"."+DMAV_BERGWERK+" b ON g.t_id=b.grundstueck"
                            +" WHERE g.egrid=?", new RowMapper<Geometry>() {
                        WKBReader decoder=new WKBReader(geomFactory);
                        
                        @Override
                        public Geometry mapRow(ResultSet rs, int rowNum) throws SQLException {
                            Geometry polygon=null;
                            byte l_geometrie[]=rs.getBytes("l_geometrie");
                            byte s_geometrie[]=rs.getBytes("s_geometrie");
                            byte b_geometrie[]=rs.getBytes("b_geometrie");
                            try {
                                if(l_geometrie!=null) {
                                    polygon=decoder.read(l_geometrie);
                                }else if(s_geometrie!=null) {
                                    polygon=decoder.read(s_geometrie);
                                }else if(b_geometrie!=null) {
                                    polygon=decoder.read(b_geometrie);
                                }else {
                                    throw new IllegalStateException("no geometrie");
                                }
                                if(polygon==null || polygon.isEmpty()) {
                                    return null;
                                }
                            } catch (ParseException e) {
                                throw new IllegalStateException(e);
                            }
                            return polygon;
                        }

                        
                    },egrid);
            if(gslist==null || gslist.isEmpty()) {
                return null;
            }
            Geometry multiPolygon=geomFactory.createMultiPolygon(gslist.toArray(new Polygon[gslist.size()]));
            return multiPolygon;
    }

    protected MultilingualBlob createMultilingualBlob(byte[] wmsImage) {
        LocalisedBlob blob=new LocalisedBlob();
        blob.setBlob(wmsImage);
        blob.setLanguage(LanguageCode.DE);
        MultilingualBlob ret=new MultilingualBlob();
        ret.getLocalisedBlob().add(blob);
        return ret;
    }

    private static final int MAP_WIDTH_MM = 174;
    private static final int MAP_HEIGHT_MM = 99;
    private void setMapBBOX(ch.ehi.av.webservice.jaxb.extractdata._1_0.Map map, Envelope bbox) {
        map.setMax(jts2xtf.createCoordType(new Coordinate(bbox.getMaxX(),bbox.getMaxY())));
        map.setMin(jts2xtf.createCoordType(new Coordinate(bbox.getMinX(),bbox.getMinY())));
    }
    private HashMap<String,PropertyType> propertyTypes=null;
    private PropertyType mapPropertyType(String gsArt) {
        if(propertyTypes==null) {
            propertyTypes=new HashMap<String,PropertyType>();
            java.util.List<java.util.Map<String,Object>> baseData=jdbcTemplate.queryForList(
                    "SELECT acode,titel_de,titel_fr,titel_it,titel_rm,titel_en FROM "+getSchema()+"."+DMKONFIG_GRUNDSTUECKSARTTXT);
            for(java.util.Map<String,Object> status:baseData) {
                MultilingualText codeTxt=createMultilingualTextType((String)status.get("titel_de"));
                PropertyType gsType=new PropertyType();
                gsType.setText(codeTxt);
                final String code = (String)status.get("acode");
                if("Liegenschaft".equals(code)) {
                    gsType.setCode(PropertyTypeCode.REAL_ESTATE);
                /*}else if("SelbstRecht.Baurecht".equals(code)) {
                    gsType.setCode(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT);
                }else if("SelbstRecht.Quellenrecht".equals(code)) {
                    gsType.setCode(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT);
                }else if("SelbstRecht.Konzessionsrecht".equals(code)) {
                    gsType.setCode(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT);
                }else if("SelbstRecht.weitere".equals(code)) {
                    gsType.setCode(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT);*/
                }else if("SelbstaendigesDauerndesRecht".equals(code)) {
                    gsType.setCode(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT);
                }else if("Bergwerk".equals(code)) {
                    gsType.setCode(PropertyTypeCode.MINE);
                }else {
                    throw new IllegalStateException("unknown code '"+code+"'");
                }
                propertyTypes.put(code,gsType);
            }
        }
        if(gsArt!=null) {
            return propertyTypes.get(gsArt);
        }
        return null;
    }
    private void setParcel(ExtractType extract, String egrid, Grundstueck parcel,Envelope bbox, boolean withGeometry,boolean withImages,int dpi) {
        WKBWriter geomEncoder=new WKBWriter(2,ByteOrderValues.BIG_ENDIAN);
        geomEncoder.write(parcel.getGeometrie());
        
        RealEstateDPR gs = new  RealEstateDPR();
        gs.setEGRID(egrid);
        final String nbident = parcel.getNbident();
        String canton=nbident.substring(0, 2);
        gs.setCanton(CantonCode.fromValue(canton));
        gs.setIdentDN(nbident);
        gs.setNumber(parcel.getNummer());
        {
    		try {
                java.util.Map<String,Object> gbKreis=jdbcTemplate.queryForMap(
                        "SELECT aname,gemeinde,egris_subkreis,egris_los FROM "+getSchema()+"."+DMAVSP_CH_V1_0UNTERENHTGRNDBUCH_GRUNDBUCHKREIS+" WHERE nbident=?",nbident);
                gs.setSubUnitOfLandRegister((String)gbKreis.get("aname"));
                gs.setMunicipalityCode((Integer)gbKreis.get("gemeinde"));
                //gs.setEgrisSubKreis((Integer)gbKreis.get("egris_subkreis"));
                //gs.setEgrisLos((Integer)gbKreis.get("egris_los"));
            }catch(EmptyResultDataAccessException ex) {
                logger.warn("no GrundbuchKreis for nbident {}",nbident);
            }
        	
        }
        if(gs.getSubUnitOfLandRegister()!=null) {
            gs.setSubUnitOfLandRegisterDesignation(subUnitOfLandRegisterDesignation);
        }
        // gemeindename
        String gemeindename=jdbcTemplate.queryForObject(
                "SELECT aname FROM "+getSchema()+"."+DMAV_GEMEINDE+" WHERE bfsnummer=?",String.class,gs.getMunicipalityCode());
        gs.setMunicipalityName(gemeindename);
        gs.setLandRegistryArea((int)parcel.getFlaechenmas());
        String gsArt=parcel.getArt();
        gs.setType(mapPropertyType(gsArt));
        //gs.setMetadataOfGeographicalBaseData(value);
        if(withGeometry) {
            MultiSurfaceType geomGml=jts2xtf.createMultiSurfaceType(parcel.getGeometrie());
            gs.setLimit(geomGml);
        }
        
        
        {
            // Planausschnitt 174 * 99 mm
        	ch.ehi.av.webservice.jaxb.extractdata._1_0.Map planForLandregister=new ch.ehi.av.webservice.jaxb.extractdata._1_0.Map();
            String fixedWmsUrl = getWmsUrl(bbox, avPlanForLandregister,dpi);
            planForLandregister.setReferenceWMS(createMultilingualUri(fixedWmsUrl));
            gs.setPlanForLandRegister(planForLandregister);
            if(withImages) {
                try {
                    planForLandregister.setImage(createMultilingualBlob(getWmsImage(fixedWmsUrl)));
                } catch (IOException | URISyntaxException e) {
                    logger.error("failed to get wms image",e);
                    planForLandregister.setImage(createMultilingualBlob(minimalImage));
                }
            }
            setMapBBOX(planForLandregister,bbox);
        }
        {
            // Planausschnitt 174 * 99 mm
        	ch.ehi.av.webservice.jaxb.extractdata._1_0.Map planForLandregisterMainPage=new ch.ehi.av.webservice.jaxb.extractdata._1_0.Map();
            String fixedWmsUrl = getWmsUrl(bbox, avPlanForLandregisterMainPage,dpi);
            planForLandregisterMainPage.setReferenceWMS(createMultilingualUri(fixedWmsUrl));
            gs.setPlanForLandRegisterMainPage(planForLandregisterMainPage);
            if(withImages) {
                try {
                    planForLandregisterMainPage.setImage(createMultilingualBlob(getWmsImage(fixedWmsUrl)));
                } catch (IOException | URISyntaxException e) {
                    logger.error("failed to get wms image",e);
                    planForLandregisterMainPage.setImage(createMultilingualBlob(minimalImage));
                }
            }
            setMapBBOX(planForLandregisterMainPage,bbox);
        }
        {
            // Planausschnitt 174 * 99 mm
        	ch.ehi.av.webservice.jaxb.extractdata._1_0.Map planForSituation=new ch.ehi.av.webservice.jaxb.extractdata._1_0.Map();
            String fixedWmsUrl = getWmsUrl(bbox, avPlanForSituation,dpi);
            planForSituation.setReferenceWMS(createMultilingualUri(fixedWmsUrl));
            gs.setPlanForSituation(planForSituation);
            if(withImages) {
                try {
                    planForSituation.setImage(createMultilingualBlob(getWmsImage(fixedWmsUrl)));
                } catch (IOException | URISyntaxException e) {
                    logger.error("failed to get wms image",e);
                    planForSituation.setImage(createMultilingualBlob(minimalImage));
                }
            }
            setMapBBOX(planForSituation,bbox);
        } 
        {
            // Geometer
        	String geometerUri=null;
        	Office office = new Office();
            office.setName(createMultilingualTextType("Geometer"));
            try {
                geometerUri=jdbcTemplate.queryForObject(
                        "SELECT av FROM "+getSchema()+"."+AV_WBSRVC_V1_0KONFIGURATION_ZUSTAENDIGESTELLE+" WHERE nbident=?",String.class,nbident);
            }catch(EmptyResultDataAccessException ex) {
                logger.error("failed to get geometer of nbident {}",nbident);
            }
            office.setOfficeAtWeb(createMultilingualUri(geometerUri));

            setOffice(office);
            gs.setResponsibleOffice(office);
        }
        
        extract.setRealEstateDPR(gs);
        
    }

    private Envelope getMapBBOX(Geometry parcelGeom) {
        Envelope bbox = parcelGeom.getEnvelopeInternal();
        double width=bbox.getWidth();
        double height=bbox.getHeight();
        double factor=Math.max(width/MAP_WIDTH_MM,height/MAP_HEIGHT_MM);
        bbox.expandBy((MAP_WIDTH_MM*factor-width)/2.0, (MAP_HEIGHT_MM*factor-height)/2.0);
        bbox.expandBy(5.0*factor, 5.0*factor);
        return bbox;
    }

    private byte[] getWmsImage(String fixedWmsUrl) 
        throws IOException, URISyntaxException 
    {
        byte ret[]=null;
        java.net.URL url=null;
        url=new java.net.URI(fixedWmsUrl).toURL();
        logger.trace("fetching <{}> ...",url);
        java.net.URLConnection conn=null;
        try {
            //
            // java  -Dhttp.proxyHost=myproxyserver.com  -Dhttp.proxyPort=80 MyJavaApp
            //
            // System.setProperty("http.proxyHost", "myProxyServer.com");
            // System.setProperty("http.proxyPort", "80");
            //
            // System.setProperty("java.net.useSystemProxies", "true");
            //
            // since 1.5 
            // Proxy instance, proxy ip = 123.0.0.1 with port 8080
            // Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("123.0.0.1", 8080));
            // URL url = new URL("http://www.yahoo.com");
            // HttpURLConnection uc = (HttpURLConnection)url.openConnection(proxy);
            // uc.connect();
            // 
            conn = url.openConnection();
        } catch (IOException e) {
            throw e;
        }
        java.io.BufferedInputStream in=null;
        java.io.ByteArrayOutputStream fos=null;
        try{
            try {
                in=new java.io.BufferedInputStream(conn.getInputStream());
            } catch (IOException e) {
                throw e;
            }
            fos = new java.io.ByteArrayOutputStream();
            try {
                byte[] buf = new byte[1024];
                int i = 0;
                while ((i = in.read(buf)) != -1) {
                    fos.write(buf, 0, i);
                }
            } catch (IOException e) {
                throw e;
            }
            fos.flush();
            ret=fos.toByteArray();
        }finally{
            if(in!=null){
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error("failed to close wms input stream",e);
                }
                in=null;
            }
            if(fos!=null){
                try {
                    fos.close();
                } catch (IOException e) {
                    logger.error("failed to close wms output stream",e);
                }
                fos=null;
            }
        }
        return ret;
    }

    private String getWmsUrl(Envelope bbox, String url,int dpi) {
        final UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url);
        UriComponents uri=builder.build();
        String paramSrs=getWmsParam(uri.getQueryParams(), WMS_PARAM_SRS);
        if(uri.getQueryParams().containsKey(paramSrs)) {
            builder.replaceQueryParam(paramSrs,"EPSG:2056");
        }
        String paramBbox=getWmsParam(uri.getQueryParams(), WMS_PARAM_BBOX);
        builder.replaceQueryParam(paramBbox, bbox.getMinX()+","+bbox.getMinY()+","+bbox.getMaxX()+","+bbox.getMaxY());
        int mapWidthPixel = (int) (dpi*MAP_WIDTH_MM/25.4);
        int mapHeightPixel = (int) (dpi*MAP_HEIGHT_MM/25.4);
        
        String paramDpi=getWmsParam(uri.getQueryParams(), WMS_PARAM_DPI);
        builder.replaceQueryParam(paramDpi, dpi);
        String paramHeight=getWmsParam(uri.getQueryParams(), WMS_PARAM_HEIGHT);
        builder.replaceQueryParam(paramHeight, mapHeightPixel);
        String paramWidth=getWmsParam(uri.getQueryParams(), WMS_PARAM_WIDTH);
        builder.replaceQueryParam(paramWidth, mapWidthPixel);
        String fixedWmsUrl = builder.build().toUriString();
        return fixedWmsUrl;
    }
    private String getWmsParam(MultiValueMap<String, String> queryParams, String param) {
        for(String queryParam:queryParams.keySet()) {
            if(queryParam.equalsIgnoreCase(param)) {
                return queryParam;
            }
        }
        return param;
    }

    private String  verifyEgrid(String egrid,String identdn,String number) {
        try {
            String ret=jdbcTemplate.queryForObject(
                    "SELECT egrid AS type FROM "+getSchema()+"."+DMAV_GRUNDSTUECK+" WHERE egrid=? OR (nummer=? AND nbident=?)", String.class,egrid,number,identdn);
            return ret;
        }catch(EmptyResultDataAccessException ex) {
        }
        return null;
    }

    private java.sql.Date getBasedatadateOfMunicipality(String nbident) {
        java.sql.Date ret=null;
        try {
            ret=jdbcTemplate.queryForObject("SELECT stand from "+getSchema()+"."+AV_WBSRVC_V1_0KONFIGURATION_METADATENAV+" WHERE nbident=?",java.sql.Date.class,nbident);
        }catch(EmptyResultDataAccessException ex) {
            // a non-unlocked municipality has no entry
            return null;
        }
        if(ret==null) {
            ret=new java.sql.Date(System.currentTimeMillis());
        }
        return ret;
    }
    
    @Scheduled(cron="0 * * * * *")
    private void cleanUp() {    
        java.io.File[] tmpDirs = new java.io.File(oerebTmpdir).listFiles();
        if(tmpDirs!=null) {
            for (java.io.File tmpDir : tmpDirs) {
                if (tmpDir.getName().startsWith(TMP_FOLDER_PREFIX)) {
                    try {
                        FileTime creationTime = (FileTime) Files.getAttribute(Paths.get(tmpDir.getAbsolutePath()), "creationTime");                    
                        Instant now = Instant.now();
                        
                        long fileAge = now.getEpochSecond() - creationTime.toInstant().getEpochSecond();
                        if (fileAge > 60*60) {
                            logger.info("deleting {}", tmpDir.getAbsolutePath());
                            FileSystemUtils.deleteRecursively(tmpDir);
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        }
    }
}