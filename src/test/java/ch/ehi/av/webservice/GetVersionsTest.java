package ch.ehi.av.webservice;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.test.context.junit4.SpringRunner;
import ch.ehi.av.webservice.jaxb.versioning._1_0.*;
import jakarta.annotation.PostConstruct;

@RunWith(SpringRunner.class)
@SpringBootTest
//@Ignore
public class GetVersionsTest {
	
    private static final String TEST_WS_OUT = GetExtractTest.TEST_WS_OUT;

    @Autowired
    AvController service;
        
    @Autowired
    Jaxb2Marshaller marshaller;
    @PostConstruct
    public void setup() throws Exception
    {
        new File(TEST_WS_OUT).mkdirs();
    }
    @Test
    public void versions() throws Exception 
    {
        Assert.assertNotNull(service);
        GetVersionsResponse response = service.getVersions();
        marshaller.marshal(response,new javax.xml.transform.stream.StreamResult(new File(TEST_WS_OUT,"versions-out.xml")));
        Assert.assertEquals(1,response.getValue().getSupportedVersion().size());
        Assert.assertEquals(AvController.SERVICE_SPEC_VERSION,response.getValue().getSupportedVersion().get(0).getVersion());
        
    }
    
}
