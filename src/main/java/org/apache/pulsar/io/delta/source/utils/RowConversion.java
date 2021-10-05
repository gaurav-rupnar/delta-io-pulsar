package org.apache.pulsar.io.delta.source.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class RowConversion {
    private static ObjectMapper mapper = new ObjectMapper();

    public static String rowRecordToJson(RowRecord row) throws JsonProcessingException {
        ObjectNode jsonNodes = convertRowRecordToJson(row);
        String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNodes);
        return jsonString;

    }

    public static ObjectNode convertRowRecordToJson(RowRecord row) throws JsonProcessingException {
        ObjectNode rootNode = mapper.createObjectNode();
        StructType schema = row.getSchema();
        for(StructField f : schema.getFields()){
            toJson(f.getName(),f.getDataType(),row,rootNode);
        }
        String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
        System.out.println(jsonString);
        return rootNode;
    }


    private static void toJson(String fieldName, DataType dataType, RowRecord row, ObjectNode rootNode) throws JsonProcessingException {

        switch(dataType.getTypeName().toLowerCase()) {


            case "int" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getInt(fieldName)) ); break;
            case "long" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getLong(fieldName)) ); break;
            case "binary" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getBinary(fieldName)) ); break;
            case "short" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getShort(fieldName)) ); break;
            case "boolean" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getBoolean(fieldName)) ); break;
            case "float" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getFloat(fieldName)) ); break;
            case "double" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getDouble(fieldName)) ); break;
            case "string" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getString(fieldName)) ); break;
            case "byte" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getByte(fieldName)) ); break;
            case "bigdecimal" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getBigDecimal(fieldName)) ); break;

            case "timestamp" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getTimestamp(fieldName).getTime()) ); break;
            case "date" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getDate(fieldName).toString()) ); break;
            //case "List" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getList((fieldName))); break;
            //case "Map" : rootNode.setAll( mapper.createObjectNode().put(fieldName, row.getMap(fieldName)) ); break;
            case "struct" : rootNode.set(fieldName,convertRowRecordToJson(row.getRecord(fieldName))); break;

        }


    }
}
