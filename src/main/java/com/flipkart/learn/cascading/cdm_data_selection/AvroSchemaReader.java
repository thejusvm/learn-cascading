package com.flipkart.learn.cascading.cdm_data_selection;

/**
 *
 /**
 * Created by subhadeep.m on 09/06/17.
 *
 * Used by shubhranshu.shekhar
 */
//public class AvroSchemaReader {
//}


import com.google.common.base.Optional;
        import lombok.AllArgsConstructor;
        import lombok.Data;
        import lombok.Getter;
        import org.apache.avro.Schema;

        import java.io.*;
        import java.util.*;

/**
 * Created by subhadeep.m on 09/06/17.
 */
public class AvroSchemaReader {

    private String inputSchemaPath;
    private Schema schema;

    @Getter
    private Map<String, List<NodeIndex>> nodeParentIndex = new HashMap<>();

    public AvroSchemaReader(String filePath) {
        this.inputSchemaPath = filePath;
    }

    public Optional<List<NodeIndex>> getNodeIndex(String nodeName) {

        if (this.nodeParentIndex.containsKey(nodeName)) {
            return Optional.of(this.nodeParentIndex.get(nodeName));
        }
        return Optional.absent();
    }

//    public Optional<NodeIndex> getNodeIndex(String nodeName, String parentNodeName) {
//
//        Optional<List<NodeIndex>> parentNodeIndices = this.getNodeIndex(nodeName);
//        if (!parentNodeIndices.isPresent()) {
//            return Optional.absent();
//        }
//        for (NodeIndex nodeIndex : parentNodeIndices.get()) {
//            if (nodeIndex.getNodeName().equals(parentNodeName)) {
//                return Optional.of(nodeIndex);
//            }
//        }
//        return Optional.absent();
//    }

    public Optional<NodeIndex> getIndex(String parentNodeName, String nodeName) {

        Optional<List<NodeIndex>> parentNodeIndices = this.getNodeIndex(nodeName);
        if (!parentNodeIndices.isPresent()) {
            return null;
        }
        for (NodeIndex nodeIndex : parentNodeIndices.get()) {
            if (nodeIndex.getNodeName().equals(parentNodeName)) {
                return Optional.of(nodeIndex);
            }
        }
        return Optional.absent();
    }

    private void parseNode(Schema nodeSchema, String nodeName, String nodeParent,
                           Integer currIndex) {

        NodeIndex nodeIndex = new NodeIndex(nodeParent, currIndex);
        if (this.nodeParentIndex.containsKey(nodeName)) {
            this.nodeParentIndex.get(nodeName).add(nodeIndex);
        }
        else {
            List<NodeIndex> nodeIndices = new ArrayList<>();
            nodeIndices.add(nodeIndex);
            this.nodeParentIndex.put(nodeName, nodeIndices);
        }

        if (nodeSchema.getType() == Schema.Type.RECORD) {
            int index = 0;
            for (Schema.Field field : nodeSchema.getFields()) {
                parseNode(field.schema(), field.name(), nodeName, index);
                index++;

            }
        }

        if (nodeSchema.getType() == Schema.Type.UNION) {
            for (Schema arrNodeSchema : nodeSchema.getTypes()) {
                if (arrNodeSchema.getType() == Schema.Type.RECORD) {
                    parseNode(arrNodeSchema, nodeName, nodeName, 0);
                }
            }
        }
    }


    public AvroSchemaReader buildSchema() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        File fileRef = new File(classLoader.getResource(this.inputSchemaPath).getFile());
        Schema.Parser parser = new Schema.Parser();
        this.schema = parser.parse(fileRef);
        this.parseNode(this.schema, "root", "-1", 0);
        return this;
    }


    public static void main(String[] args) {
        AvroSchemaReader avroSchemaReader = new AvroSchemaReader("data/nonavro/impressionppvSchema.avsc");
        try {
            avroSchemaReader.buildSchema();

            System.out.println(avroSchemaReader.getNodeIndex("listingId").get());
//            System.out.println(avroSchemaReader.getIndex("searchAttributes", "searchSessionId").get().getIdx());
        }
        catch (IOException e) {
            System.out.println("error reading the file");
            e.printStackTrace();
        }

    }

    @Data
    @AllArgsConstructor
    public static class NodeIndex {
        private String nodeName;
        private Integer idx;
    }

}