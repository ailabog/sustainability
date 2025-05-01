package auto.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.*;

public class KafkaProducerSetup {

    public Properties setProducerProps(String bootstrapServers, String authInfo, String jaasConfig, String trustStoreLocation, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", authInfo);
        props.put("sasl.jaas.config", jaasConfig);
        props.put("ssl.truststore.location", trustStoreLocation);
        props.put("ssl.truststore.password", "changeit");
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("schema.registry.ssl.truststore.location", trustStoreLocation);
        props.put("schema.registry.ssl.truststore.password", "changeit");
        props.put("auto.register.schemas", false);
        props.put("specific.avro.reader", true);
        return props;
    }

    public static GenericRecord produceMessage(String eventType, String eventSubtype, String currentDateComplete, String evId, String evSDesc, String eventSource, String docT, String BArea, String batchNoPfx, String objectStore, String mimeType, String sysId, String docClass, String GUID) {

        String avroSchema = "{\"type\":\"record\",\"name\":\"eDocPiublish\",\"namespace\":\"streaming\",\"fields\":[{\"name\":\"eventheader\",\"type\":{\"type\":\"record\",\"name\":\"EventHeader\",\"fields\":[{\"name\":\"eventType\",\"type\":\"string\",\"doc\":\"enterpriseservices\"},{\"name\":\"eventSubtype\",\"type\":\"string\",\"doc\":\"documentaddupdate\"},{\"name\":\"eventDateTime\",\"type\":\"string\"},{\"name\":\"eventGeneratedDateTime\",\"type\":\"string\"},{\"name\":\"evId\",\"type\":[\"string\",\"null\"]},{\"name\":\"eventRequestId\",\"type\":[\"string\",\"null\"]},{\"name\":\"evSDesc\",\"type\":[\"string\",\"null\"]},{\"name\":\"eventSource\",\"type\":[\"string\",\"null\"],\"doc\":\"The producer of this message.\"},{\"name\":\"metadata\",\"type\":{\"type\":\"record\",\"name\":\"metadata\",\"fields\":[]}}]}},{\"name\":\"eventBody\",\"type\":{\"type\":\"record\",\"name\":\"EventBody\",\"fields\":[{\"name\":\"Document_Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"BArea\",\"type\":[\"string\",\"null\"]},{\"name\":\"Activity_Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"Batch_No_Pfx\",\"type\":[\"string\",\"null\"]},{\"name\":\"SuppressImageIndicator\",\"type\":[\"string\",\"null\"]},{\"name\":\"ObjectStore\",\"type\":\"string\"},{\"name\":\"Doc_Control_Number\",\"type\":[\"string\",\"null\"]},{\"name\":\"PackageInProcess\",\"type\":[\"string\",\"null\"]},{\"name\":\"MimeType\",\"type\":\"string\"},{\"name\":\"sysId\",\"type\":[\"string\",\"null\"]},{\"name\":\"Transaction_ID\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}},{\"name\":\"primary_h_id1\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}},{\"name\":\"AdminSys\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}},{\"name\":\"TrackingNumber\",\"type\":[\"string\",\"null\"]},{\"name\":\"DateCreated\",\"type\":\"string\"},{\"name\":\"DocClass\",\"type\":\"string\"},{\"name\":\"VSID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"GUID\",\"type\":\"string\"},{\"name\":\"MajorVersion\",\"type\":\"string\"},{\"name\":\"MinorVersion\",\"type\":\"string\"},{\"name\":\"DeliveryIndicator\",\"type\":[\"string\",\"null\"]},{\"name\":\"SensitivePartyIndicator\",\"type\":[\"string\",\"null\"]},{\"name\":\"publishType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"propertiesList\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PropertyVariable\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"multiValue\",\"type\":\"string\"},{\"name\":\"multiList\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}}]}}}]}}]}";

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchemaInv = parser.parse(avroSchema);

        GenericRecord avroRecord = new GenericData.Record(avroSchemaInv);

        GenericRecord eventHeader = new GenericData.Record(avroSchemaInv.getField("eventheader").schema());
        eventHeader.put("eventType", eventType);
        eventHeader.put("eventSubtype", eventSubtype);
        eventHeader.put("eventDateTime", currentDateComplete);
        eventHeader.put("eventGeneratedDateTime", currentDateComplete);
        eventHeader.put("evId", evId);
        eventHeader.put("eventRequestId", evId);
        eventHeader.put("evSDesc", evSDesc);
        eventHeader.put("eventSource", eventSource);
        eventHeader.put("metadata", new HashMap<>());

        GenericRecord eventBody = new GenericData.Record(avroSchemaInv.getField("eventBody").schema());

        eventBody.put("Document_Type", docT);
        eventBody.put("BArea", BArea);
        eventBody.put("Batch_No_Pfx", batchNoPfx);
        eventBody.put("ObjectStore", objectStore);
        eventBody.put("MimeType", mimeType);

        eventBody.put("sysId", sysId);
        eventBody.put("Transaction_ID", new GenericData.Array<>(avroSchemaInv.getField("eventBody").schema().getField("Transaction_ID").schema(), Arrays.asList("000000")));
        eventBody.put("primary_h_id1", new GenericData.Array<>(avroSchemaInv.getField("eventBody").schema().getField("primary_h_id1").schema(), Arrays.asList("00000")));
        eventBody.put("AdminSys", new GenericData.Array<>(avroSchemaInv.getField("eventBody").schema().getField("AdminSys").schema(), Arrays.asList("LOL")));
        eventBody.put("DateCreated", currentDateComplete);
        eventBody.put("DocClass", docClass);
        eventBody.put("GUID", GUID);
        eventBody.put("MajorVersion", "1");
        eventBody.put("MinorVersion", "0");

        //propertiesList implementation
        List<GenericRecord> propertiesList = new ArrayList<>();

        Schema propertiesListSchema = avroSchemaInv.getField("eventBody").schema().getField("propertiesList").schema().getElementType();
        GenericRecord propertyRecord = new GenericData.Record(propertiesListSchema);

        propertyRecord.put("name", "PropertyName");
        propertyRecord.put("value", "PropertyValue");
        propertyRecord.put("type", "PropertyType");
        propertyRecord.put("multiValue", "PropertyMultiValue");
        propertyRecord.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("Value1", "Value2")));
        propertiesList.add(propertyRecord);
        eventBody.put("propertiesList", propertiesList);

        avroRecord.put("eventheader", eventHeader);
        avroRecord.put("eventBody", eventBody);

        System.out.println("Inv message produced: " + eventHeader + eventBody);
        return avroRecord;
    }

    public static GenericRecord produceMessageComposeAgreementKey(String eventType, String eventSubtype, String currentDateComplete, String evId, String evSDesc, String eventSource, String docT, String BArea, String batchNoPfx, String objectStore, String mimeType, String sysId, String docClass, String GUID) {

        String avroSchema = "{\"type\":\"record\",\"name\":\"eDPublish\",\"namespace\":\"streaming\",\"fields\":[{\"name\":\"eventheader\",\"type\":{\"type\":\"record\",\"name\":\"EventHeader\",\"fields\":[{\"name\":\"eventType\",\"type\":\"string\",\"doc\":\"enterpriseservices\"},{\"name\":\"eventSubtype\",\"type\":\"string\",\"doc\":\"documentaddupdate\"},{\"name\":\"eventDateTime\",\"type\":\"string\"},{\"name\":\"eventGeneratedDateTime\",\"type\":\"string\"},{\"name\":\"evId\",\"type\":[\"string\",\"null\"]},{\"name\":\"eventRequestId\",\"type\":[\"string\",\"null\"]},{\"name\":\"evSDesc\",\"type\":[\"string\",\"null\"]},{\"name\":\"eventSource\",\"type\":[\"string\",\"null\"],\"doc\":\"The producer of this message.\"},{\"name\":\"metadata\",\"type\":{\"type\":\"record\",\"name\":\"metadata\",\"fields\":[]}}]}},{\"name\":\"eventBody\",\"type\":{\"type\":\"record\",\"name\":\"EventBody\",\"fields\":[{\"name\":\"Document_Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"BArea\",\"type\":[\"string\",\"null\"]},{\"name\":\"Activity_Type\",\"type\":[\"string\",\"null\"]},{\"name\":\"Batch_No_Pfx\",\"type\":[\"string\",\"null\"]},{\"name\":\"SuppressImageIndicator\",\"type\":[\"string\",\"null\"]},{\"name\":\"ObjectStore\",\"type\":\"string\"},{\"name\":\"Doc_Control_Number\",\"type\":[\"string\",\"null\"]},{\"name\":\"PackageInProcess\",\"type\":[\"string\",\"null\"]},{\"name\":\"MimeType\",\"type\":\"string\"},{\"name\":\"sysId\",\"type\":[\"string\",\"null\"]},{\"name\":\"Transaction_ID\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}},{\"name\":\"primary_h_id1\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}},{\"name\":\"AdminSys\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}},{\"name\":\"TrackingNumber\",\"type\":[\"string\",\"null\"]},{\"name\":\"DateCreated\",\"type\":\"string\"},{\"name\":\"DocClass\",\"type\":\"string\"},{\"name\":\"VSID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"GUID\",\"type\":\"string\"},{\"name\":\"MajorVersion\",\"type\":\"string\"},{\"name\":\"MinorVersion\",\"type\":\"string\"},{\"name\":\"DeliveryIndicator\",\"type\":[\"string\",\"null\"]},{\"name\":\"SensitivePartyIndicator\",\"type\":[\"string\",\"null\"]},{\"name\":\"publishType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"propertiesList\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PropertyVariable\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"multiValue\",\"type\":\"string\"},{\"name\":\"multiList\",\"type\":{\"type\":\"array\",\"items\":[\"string\",\"null\"]}}]}}}]}}]}";

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchemaInv = parser.parse(avroSchema);

        GenericRecord avroRecord = new GenericData.Record(avroSchemaInv);
        //  log.info(avroRecord.toString())

        GenericRecord eventHeader = new GenericData.Record(avroSchemaInv.getField("eventheader").schema());
        eventHeader.put("eventType", eventType);
        eventHeader.put("eventSubtype", eventSubtype);
        eventHeader.put("eventDateTime", currentDateComplete);
        eventHeader.put("eventGeneratedDateTime", currentDateComplete);
        eventHeader.put("evId", evId);
        eventHeader.put("eventRequestId", evId);
        eventHeader.put("evSDesc", evSDesc);
        eventHeader.put("eventSource", eventSource);
        eventHeader.put("metadata", new HashMap<>());

        GenericRecord eventBody = new GenericData.Record(avroSchemaInv.getField("eventBody").schema());

        eventBody.put("Document_Type", docT);
        eventBody.put("BArea", BArea);
        eventBody.put("Batch_No_Pfx", batchNoPfx);
        eventBody.put("ObjectStore", objectStore);
        eventBody.put("MimeType", mimeType);
        eventBody.put("sysId", sysId);
        eventBody.put("Transaction_ID", new GenericData.Array<>(avroSchemaInv.getField("eventBody").schema().getField("Transaction_ID").schema(), Arrays.asList("000")));
        eventBody.put("primary_h_id1", new GenericData.Array<>(avroSchemaInv.getField("eventBody").schema().getField("primary_h_id1").schema(), Arrays.asList("00000")));
        eventBody.put("AdminSys", new GenericData.Array<>(avroSchemaInv.getField("eventBody").schema().getField("AdminSys").schema(), Arrays.asList("LOL1")));
        eventBody.put("DateCreated", currentDateComplete);
        eventBody.put("DocClass", docClass);
        eventBody.put("GUID", GUID);
        eventBody.put("MajorVersion", "1");
        eventBody.put("MinorVersion", "0");

//propertiesList implementation
        List<GenericRecord> propertiesList = new ArrayList<>();

        Schema propertiesListSchema = avroSchemaInv.getField("eventBody").schema().getField("propertiesList").schema().getElementType();
        GenericRecord propertyRecord = new GenericData.Record(propertiesListSchema);

        propertyRecord.put("name", "PropertyName");
        propertyRecord.put("value", "PropertyValue");
        propertyRecord.put("type", "PropertyType");
        propertyRecord.put("multiValue", "PropertyMultiValue");
        propertyRecord.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("Value1", "Value2")));
        propertiesList.add(propertyRecord);
        eventBody.put("propertiesList", propertiesList);

        GenericRecord PrimaryHoldingIDProperty = new GenericData.Record(propertiesListSchema);

        PrimaryHoldingIDProperty.put("name", "primary_h_id1");
        PrimaryHoldingIDProperty.put("value", "00000");
        PrimaryHoldingIDProperty.put("type", "String");
        PrimaryHoldingIDProperty.put("multiValue", "1");
        PrimaryHoldingIDProperty.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("00000")));

        propertiesList.add(PrimaryHoldingIDProperty);

        GenericRecord SuplimentalHoldingIDProperty = new GenericData.Record(propertiesListSchema);

        //Populate fields for Supplemental Holding ID
        SuplimentalHoldingIDProperty.put("name", "s_holding_id1");
        SuplimentalHoldingIDProperty.put("value", "VA");
        SuplimentalHoldingIDProperty.put("type", "String");
        SuplimentalHoldingIDProperty.put("multiValue", "1");
        SuplimentalHoldingIDProperty.put("multiList", new GenericData.Array<>(propertiesListSchema.getField("multiList").schema(), Arrays.asList("VA")));

        propertiesList.add(SuplimentalHoldingIDProperty);

        //Add propertiesList to eventBody
        eventBody.put("propertiesList", propertiesList);

        avroRecord.put("eventheader", eventHeader);
        avroRecord.put("eventBody", eventBody);

        System.out.println("Inv message produced: " + eventHeader + eventBody);
        return avroRecord;
    }
}
