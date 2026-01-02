package ch.ehi.av.webservice;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.xml.MarshallingHttpMessageConverter;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.filter.ForwardedHeaderFilter;

@Configuration
public class WsConfig {
    @Bean
    public ForwardedHeaderFilter forwardedHeaderFilter() {
        return new ForwardedHeaderFilter();
    }
    
    @Bean
    public HttpMessageConverter<Object> createXmlHttpMessageConverter(Jaxb2Marshaller marshaller) {
        //Jaxb2RootElementHttpMessageConverter xmlConverter = new Jaxb2RootElementHttpMessageConverter();
        MarshallingHttpMessageConverter xmlConverter = 
          new MarshallingHttpMessageConverter();
        // Jaxb2Marshaller marshaller = createMarshaller();
        xmlConverter.setMarshaller(marshaller);
        xmlConverter.setUnmarshaller(marshaller);
        return xmlConverter;
    }

    @Bean
    public Jaxb2Marshaller createMarshaller() {
        Jaxb2Marshaller marshaller=new Jaxb2Marshaller();
        //marshaller.setClassesToBeBound(ch.ehi.oereb.schemas.oereb._1_0.versioning.ObjectFactory.class,ch.ehi.oereb.schemas.oereb._1_0.extract.ObjectFactory.class);
        marshaller.setPackagesToScan("ch.ehi.av.webservice.jaxb");
        marshaller.setSupportJaxbElementClass(true);
        marshaller.setLazyInit(true);
        return marshaller;
    }
    //@Bean 
    //public ch.so.agi.oereb.pdf4oereb.Converter createExtractXml2pdfConverter(){
    //    return new ch.so.agi.oereb.pdf4oereb.Converter();
    //}
}
