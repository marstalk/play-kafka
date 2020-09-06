package com.marstalk.kfk.department;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser {
    public static void main(String[] args) throws IOException {
        // read json file
        try(FileChannel fileChannel = FileChannel.open(Paths.get("C:\\Users\\jiach\\Desktop\\department-tree.json"))){
            ByteBuffer byteBuffer = ByteBuffer.allocate((int)fileChannel.size());
            fileChannel.read(byteBuffer);
            byte[] bytes = new byte[(int)fileChannel.size()];
            byteBuffer.flip();
            byteBuffer.get(bytes);
            String jsonStr = "";
            jsonStr += new String(bytes, StandardCharsets.UTF_8);
//            System.out.println(jsonStr);

            final JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            Map<Integer, Node> map = new HashMap<>();

            parseObject(jsonObject, map);

//            System.out.println(map);
            final ArrayList<Node> nodes = new ArrayList<>();
            getAllNodes(map, 20204390, nodes);
            System.out.println(nodes);
            nodes.clear();

            getAllNodes(map, 20101105, nodes);
            System.out.println(nodes);
        }
    }

    private static void getAllNodes(Map<Integer, Node> map, Integer i, ArrayList<Node> nodes) {
        if (i == null) {
            return;
        }
        final Node node = map.get(i);
        if (node != null) {
            nodes.add(node);
            getAllNodes(map, node.getParentId(), nodes);
        }
    }

    private static void parseObject(JSONObject jsonObject, Map<Integer, Node> map) {
        final Integer id = jsonObject.getInteger("id");
        final Integer parentId = jsonObject.getInteger("parentId");
        final String name = jsonObject.getString("name");
        final Node node = new Node(id, name, parentId);
        map.put(id, node);
        final JSONArray children = jsonObject.getJSONArray("children");
        if (children != null && !children.isEmpty()) {
            for (int i = 0; i < children.size(); i++) {
                final JSONObject jsonObject1 = children.getJSONObject(i);
                parseObject(jsonObject1, map);
            }
        }
    }
}

class Node{
    private Integer id;
    private String name;
    private Integer parentId;

    public Node(Integer id, String name, Integer parentId) {
        this.id = id;
        this.name = name;
        this.parentId = parentId;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getParentId() {
        return parentId;
    }

    public void setParentId(Integer parentId) {
        this.parentId = parentId;
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", parentId=" + parentId +
                '}';
    }
}
