package ch.ehi.av.webservice;

import java.io.File;
import java.sql.Connection;
import jakarta.annotation.PostConstruct;
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
import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2pg.PgMain;

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
                config.setModels("AV_WebService_V1_0;DMAV_Grundstuecke_V1_0;DMAV_HoheitsgrenzenAV_V1_0;DMAVSUP_UntereinheitGrundbuch_V1_0");
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
    //@Test
    // CH740632871570 Liegenschaft in Gemeinde (2500) ohne OEREB Themen
    //@Ignore("requires sql fixing")
    public void Liegenschaft_in_Gemeinde_ohne_OEREB_Themen() throws Exception 
    {
    }
    // CH580632068782 SDR mit OEREBs (P,L,F) plus eine angeschnitten
    @Test
    public void SDR_mitGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH580632068782",null,false,false,false,200);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"CH580632068782-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"CH580632068782.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        //System.out.println(diff.toString());
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        Assert.assertFalse(diffs.hasDifferences());
    }
    @Test
    public void SDR_ohneGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithoutGeometryByEgrid("xml","CH580632068782",null,false,false,false,200);
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"CH580632068782-noGeom-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"CH580632068782-noGeom.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        //System.out.println(diff.toString());
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        Assert.assertFalse(diffs.hasDifferences());
    }
    @Test
    public void SDR_ohneGeometrie_mitBild() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithoutGeometryByEgrid("xml","CH580632068782",null,false,false,true,200);
        Assert.assertEquals(200, response.getStatusCode().value());
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"CH580632068782-noGeom-mitBild-out.xml")));
    }

    // CH133289063542 Liegenschaft ohne OEREBs, keine anderen OEREBs im sichtbaren Bereich
    @Test
    public void Liegenschaft_ohneOEREBs() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH133289063542",null,false,false,false,200);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"CH133289063542-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"CH133289063542.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        //Assert.assertFalse(diffs.hasDifferences());
    }
    // CH793281100623 Liegenschaft ohne OEREBs, aber alle OEREBs von im sichtbaren Bereich (otherLegends)
    @Test
    public void Liegenschaft_otherLegends() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetExtractByIdResponse> response = (ResponseEntity<GetExtractByIdResponse>) service.getExtractWithGeometryByEgrid("xml","CH793281100623",null,false,false,false,200);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"CH793281100623-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"CH793281100623.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        //System.out.println(diff.toString());
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        //Assert.assertFalse(diffs.hasDifferences());
    }
    @Test
    public void egrid_mitGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByNumber(true,"SO0200002498","514");
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-CH580632068782-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"egrid-CH580632068782.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        Assert.assertFalse(diffs.hasDifferences());
    }
    @Test
    public void egrid_ohnGeometrie() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByNumber(false,"SO0200002498","514");
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-CH580632068782-noGeom-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"egrid-CH580632068782-noGeom.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        Assert.assertFalse(diffs.hasDifferences());
    }
    // EN=2638380.0,1251430.0
    @Test
    public void egrid_xy() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByXY(false,"2638380.0,1251430.0",null);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-xy-out.xml")));
        File controlFile = new File(TEST_EXPECTED,"egrid-xy.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc = dbf.newDocumentBuilder().newDocument(); 
        marshaller.marshal(response.getBody(), new javax.xml.transform.dom.DOMResult(doc));
        //Assert.assertThat(doc,createMatcher(controlFile));
        Diff diffs = DiffBuilder
        .compare(controlFile)
        .withTest(doc)
        .withDifferenceEvaluator(DifferenceEvaluators.chain(new PlaceholderDifferenceEvaluator(), DifferenceEvaluators.downgradeDifferencesToSimilar(ComparisonType.NAMESPACE_PREFIX)))
        .ignoreComments()
        .ignoreWhitespace()
        .checkForSimilar()
        .build();
        for(Difference diff:diffs.getDifferences()) {
            System.out.println(diff.toString());
        }
        Assert.assertFalse(diffs.hasDifferences());
    }
    @Test
    public void egrid_adr() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByAddress(false,4655,"Kirchfeldstrasse");
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-adr-out.xml")));
    }
    @Test
    public void egrid_adr_nr() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByAddress(false,4655,"Kirchfeldstrasse","8");
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-adr-nr-out.xml")));
    }
    @Test
    public void egrid_egid() throws Exception 
    {
        Assert.assertNotNull(service);
        ResponseEntity<GetEGRIDResponse> response = (ResponseEntity<GetEGRIDResponse>) service.getEgridByEgid(false,502360563);
        marshaller.marshal(response.getBody(),new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"egrid-egid-out.xml")));
    }
    @Test
    public void dbschema() throws Exception 
    {
    }
    
}
