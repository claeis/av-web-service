package ch.ehi.av.webservice;

import java.io.File;
import java.sql.Connection;
import java.util.List;

import jakarta.annotation.PostConstruct;
import jakarta.xml.bind.JAXBElement;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.test.context.junit4.SpringRunner;
import org.w3c.dom.Document;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.ComparisonType;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.Difference;
import org.xmlunit.diff.DifferenceEvaluators;
import org.xmlunit.placeholder.PlaceholderDifferenceEvaluator;

import ch.ehi.av.webservice.jaxb.extract._1_0.GetEGRIDResponse;
import ch.ehi.av.webservice.jaxb.extract._1_0.GetExtractByIdResponse;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.Building;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.BuildingEntrance;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LandCover;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LandCoverType;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.LandCoverTypeCode;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.Mutation;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.PropertyType;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.PropertyTypeCode;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.SingleObject;
import ch.ehi.av.webservice.jaxb.extractdata._1_0.SingleObjectTypeCode;
import ch.ehi.av.webservice.jaxb.geometry._1_0.MultiSurfaceType;
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2pg.PgMain;

// gradlew test --tests GetExtractTest.dbschema
// -Ddburl=jdbc:postgresql:dbname -Ddbusr=user -Ddbpwd=userpwd
@RunWith(SpringRunner.class)
@SpringBootTest
//@Ignore
public class GetExtractTest {
    private static final String TEST_ILI = "src/test/ili";
    private static final String TEST_XTF = "src/test/data";
    private static final String TEST_EXPECTED = "src/test/data-expected";
    private static final String TEST_ILI2DB_OUT = "build/ili2db";
    static final String TEST_WS_OUT = "build/ws-out";
    private static final String MODEL_DIR=Ili2db.ILI_FROM_DB+ch.interlis.ili2c.Main.ILIDIR_SEPARATOR+TEST_ILI; // +ch.interlis.ili2c.Main.ILIDIR_SEPARATOR+ch.interlis.ili2c.Main.ILI_REPOSITORY); 

    @Autowired
    AvController service;
        
    @Autowired
    Jaxb2Marshaller marshaller;

    @Autowired
    JdbcTemplate jdbcTemplate;
    
    @Value("${avws.dbschema}")
    private String DBSCHEMA;
    
    
    @PostConstruct
    public void setup() throws Exception
    {
        new File(TEST_ILI2DB_OUT).mkdirs();
        new File(TEST_WS_OUT).mkdirs();
        Connection connection = null;
        try {
            connection=jdbcTemplate.getDataSource().getConnection();
            connection.setAutoCommit(false);
            jdbcTemplate.execute("DROP SCHEMA IF EXISTS "+DBSCHEMA+" CASCADE");
            {        
                Config config=new Config();
                new PgMain().initConfig(config);
                config.setJdbcConnection(connection);
                config.setDbschema(DBSCHEMA);
                config.setLogfile(new File(TEST_ILI2DB_OUT,"ili23-import.log").getPath());
                config.setFunction(Config.FC_SCHEMAIMPORT);
                // --strokeArcs --createFk --createFkIdx --createGeomIdx   --createTidCol --createBasketCol --createImportTabs --createMetaInfo 
                // --disableNameOptimization --defaultSrsCode 2056
                // --models DM01AVCH24LV95D;PLZOCH1LV95D
                Config.setStrokeArcs(config,Config.STROKE_ARCS_ENABLE);
                config.setCreateFk(Config.CREATE_FK_YES);
                config.setCreateFkIdx(Config.CREATE_FKIDX_YES);
                config.setValue(Config.CREATE_GEOM_INDEX,Config.TRUE);
                config.setTidHandling(Config.TID_HANDLING_PROPERTY);
                config.setBasketHandling(Config.BASKET_HANDLING_READWRITE);
                config.setCreateTypeDiscriminator(Config.CREATE_TYPE_DISCRIMINATOR_ALWAYS);
                config.setCreateImportTabs(true);
                config.setCreateMetaInfo(true);
                config.setNameOptimization(Config.NAME_OPTIMIZATION_DISABLE);
                config.setDefaultSrsAuthority("EPSG");
                config.setDefaultSrsCode("2056");
                config.setModels("OfficialIndexOfAddresses_V2_2");
                config.setModeldir(MODEL_DIR); 
                Ili2db.readSettingsFromDb(config);
                Ili2db.run(config,null);
                connection.commit();
            }

            {        
                Config config=new Config();
                new PgMain().initConfig(config);
                config.setJdbcConnection(connection);
                config.setDbschema(DBSCHEMA);
                config.setLogfile(new File(TEST_ILI2DB_OUT,"ili24-import.log").getPath());
                config.setFunction(Config.FC_SCHEMAIMPORT);
                //config.setModels("OeREBKRM_V2_0;OeREBKRMtrsfr_V2_0;OeREBKRMkvs_V2_0");
                config.setModels("AV_WebService_V1_0;DMAV_Grundstuecke_V1_1;DMAV_HoheitsgrenzenAV_V1_0;DMAV_Nomenklatur_V1_1;DMAV_Bodenbedeckung_V1_1;DMAV_Einzelobjekte_V1_1;DMAVSUP_UntereinheitGrundbuch_V1_1");
                config.setModeldir(MODEL_DIR); 
                Ili2db.readSettingsFromDb(config);
                Ili2db.run(config,null);
                connection.commit();
            }
            {
                // AV-Daten
            	EhiLogger.getInstance().setTraceFilter(false);
            	File data=new File(TEST_XTF,"av_test.xtf");
                importFile(data);
            }
            {
                // Gebaeudeadressen-Daten
            	EhiLogger.getInstance().setTraceFilter(false);
            	File data=new File(TEST_XTF,"gebaddr_test.xtf");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Texte.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_MetadatenAV.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Amt.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_ZustaendigeStelle.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Information.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Logo-ch.pi.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Logo-ch.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Logo-ch.SO.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Logo-ch.2498.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Logo-ch.2500.xml");
                importFile(data);
            }
            {
                File data=new File(TEST_XTF,"AV_WebService_V1_0_Logo-ch.2502.xml");
                importFile(data);
            }
        }finally {
            if(connection!=null) {
                connection.close();
                connection=null;
            }
        }
    }
    public void importFile(File data) throws Exception {
        
        Connection connection = null;
        try {
            Config config=new Config();
            new PgMain().initConfig(config);
            connection = jdbcTemplate.getDataSource().getConnection();
            connection.setAutoCommit(false);
            config.setJdbcConnection(connection);
            config.setDbschema(DBSCHEMA);
            config.setLogfile(new File(TEST_ILI2DB_OUT,data.getName()+"-import.log").getPath());
            config.setXtffile(data.getPath());
            if(Ili2db.isItfFilename(data.getPath())){
                config.setItfTransferfile(true);
            }
            config.setFunction(Config.FC_IMPORT);
            config.setDatasetName(ch.ehi.basics.view.GenericFileFilter.stripFileExtension(data.getName()));
            config.setImportTid(true);
            config.setModeldir(MODEL_DIR); 
            Ili2db.readSettingsFromDb(config);
            config.setValidation(false);
            Ili2db.run(config,null);
            connection.commit();
        }finally {
            if(connection!=null) {
                connection.close();
                connection=null;
            }
        }
    }
    public static org.xmlunit.matchers.CompareMatcher createMatcher(File controlFile) {
        return org.xmlunit.matchers.CompareMatcher.isSimilarTo(controlFile).ignoreWhitespace().ignoreComments();
        /*
            You can now try ${xmlunit.ignore} in XMLUnit 2.6.0 (add dependency xmlunit-placeholders). Sample code is as below.

            Diff diff = DiffBuilder
            .compare(expectedXML)
            .withTest(actualXML)
            .withDifferenceEvaluator(new PlaceholderDifferenceEvaluator())
            .build();
             */
    }
    @Test
    public void SDR_mitGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH580632068782",null,false,false,false,200);
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"CH580632068782-out.xml")));
        {
            java.util.List<String> toponyms=response.getBody().getValue().getExtract().getValue().getRealEstateDPR().getToponym();
            Assert.assertEquals(1,toponyms.size());
        	Assert.assertTrue(toponyms.contains("Rosenfluh"));
        }
        {
            java.util.List<LandCover> landcovers=response.getBody().getValue().getExtract().getValue().getRealEstateDPR().getLandCover();
            Assert.assertEquals(1,landcovers.size());
            LandCover landcover=landcovers.get(0);
        	Assert.assertEquals(LandCoverTypeCode.VEGETATED_ARABLE_MEADOW_PASTURE,landcover.getType().getCode());
        	Assert.assertEquals(600,landcover.getArea());
        	Assert.assertEquals(Integer.valueOf(1701805),landcover.getEGID());
        }
        {
            java.util.List<SingleObject> singleobjects=response.getBody().getValue().getExtract().getValue().getRealEstateDPR().getSingleObject();
            Assert.assertEquals(3,singleobjects.size());
            SingleObject singleobject=singleobjects.get(0);
        	Assert.assertEquals(SingleObjectTypeCode.WALL,singleobject.getType().getCode());
        	Assert.assertEquals(Integer.valueOf(502360563),singleobject.getEGID());
        }
        {
            java.util.List<Building> buildings=response.getBody().getValue().getExtract().getValue().getRealEstateDPR().getBuilding();
            Assert.assertEquals(2,buildings.size());
            Building building=buildings.get(0);
        	Assert.assertEquals(1701805,building.getEGID());
        	java.util.List<BuildingEntrance> entrances=building.getBuildingEntrance();
            Assert.assertEquals(1,entrances.size());
        }
        {
            java.util.List<Mutation> mutations=response.getBody().getValue().getExtract().getValue().getRealEstateDPR().getMutation();
            Assert.assertEquals(2,mutations.size());
            {
                Mutation mut=mutations.get(0);
            	Assert.assertEquals("3",mut.getNummer());
            	Assert.assertEquals("SO0200002498",mut.getNBIdent());
            	Assert.assertEquals(0,mut.getDeletedParcel().size());
            	Assert.assertEquals(2,mut.getProjectedProperty().size());
            	Assert.assertEquals("CH710605328767",mut.getProjectedProperty().get(0).getEGRID());
            	Assert.assertEquals("CH287822167756",mut.getProjectedProperty().get(1).getEGRID());
            }
            {
                Mutation mut=mutations.get(1);
            	Assert.assertEquals("4",mut.getNummer());
            	Assert.assertEquals("SO0200002498",mut.getNBIdent());
            	Assert.assertEquals(1,mut.getDeletedParcel().size());
            	Assert.assertEquals("CH580632068782",mut.getDeletedParcel().get(0));
            	Assert.assertEquals(0,mut.getProjectedProperty().size());
            }
        }
    }
    @Test
    public void egrid_mitGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByNumber(true,"SO0200002498","514");
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-CH580632068782-out.xml")));
        List<JAXBElement<?>> values = response.getBody().getValue().getEgridAndNumberAndIdentDN();
		Assert.assertEquals(5,values.size());
		Assert.assertEquals("CH580632068782",((JAXBElement<String>)values.get(0)).getValue());
		Assert.assertEquals("514",((JAXBElement<String>)values.get(1)).getValue());
		Assert.assertEquals("SO0200002498",((JAXBElement<String>)values.get(2)).getValue());
		Assert.assertEquals(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT,((JAXBElement<PropertyType>)values.get(3)).getValue().getCode());
		Assert.assertEquals(1,((JAXBElement<MultiSurfaceType>)values.get(4)).getValue().getSurface().size());
    }
    @Test
    public void egrid_ohnGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByNumber(false,"SO0200002498","514");
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-CH580632068782-noGeom-out.xml")));
        List<JAXBElement<?>> values = response.getBody().getValue().getEgridAndNumberAndIdentDN();
		Assert.assertEquals(4,values.size());
		Assert.assertEquals("CH580632068782",((JAXBElement<String>)values.get(0)).getValue());
		Assert.assertEquals("514",((JAXBElement<String>)values.get(1)).getValue());
		Assert.assertEquals("SO0200002498",((JAXBElement<String>)values.get(2)).getValue());
		Assert.assertEquals(PropertyTypeCode.DISTINCT_PERMANENT_RIGHT,((JAXBElement<PropertyType>)values.get(3)).getValue().getCode());
        
    }
    // EN=2638380.0,1251430.0
    @Test
    public void egrid_xy() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByXY(false,"2638380.0,1251430.0",null);
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-xy-out.xml")));
        List<JAXBElement<?>> values = response.getBody().getValue().getEgridAndNumberAndIdentDN();
		Assert.assertEquals(12,values.size());
		Assert.assertEquals("CH710605328767",((JAXBElement<String>)values.get(0)).getValue());
		Assert.assertEquals("CH186032068919",((JAXBElement<String>)values.get(4)).getValue());
		Assert.assertEquals("CH580632068782",((JAXBElement<String>)values.get(8)).getValue());
    }
    @Test
    public void egrid_adr() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByAddress(false,4655,"Kirchfeldstrasse");
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-adr-out.xml")));
        List<JAXBElement<?>> values = response.getBody().getValue().getEgridAndNumberAndIdentDN();
		Assert.assertEquals(12,values.size());
		Assert.assertEquals("CH710605328767",((JAXBElement<String>)values.get(0)).getValue());
		Assert.assertEquals("CH186032068919",((JAXBElement<String>)values.get(4)).getValue());
		Assert.assertEquals("CH580632068782",((JAXBElement<String>)values.get(8)).getValue());
    }
    @Test
    public void egrid_adr_nr() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByAddress(false,4655,"Kirchfeldstrasse","8");
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-adr-nr-out.xml")));
        List<JAXBElement<?>> values = response.getBody().getValue().getEgridAndNumberAndIdentDN();
		Assert.assertEquals(4,values.size());
		Assert.assertEquals("CH740632871570",((JAXBElement<String>)values.get(0)).getValue());
    }
    @Test
    public void egrid_egid() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByEgid(false,502360563);
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-egid-out.xml")));
        List<JAXBElement<?>> values = response.getBody().getValue().getEgridAndNumberAndIdentDN();
		Assert.assertEquals(4,values.size());
		Assert.assertEquals("CH740632871570",((JAXBElement<String>)values.get(0)).getValue());
    }
    @Test
    public void dbschema() throws Exception 
    {
    }
    
}
