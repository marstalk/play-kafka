package com.marstalk.kfk.department;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author laizhongbin
 * @version 1.0
 * @Package org.example.test
 * @Description
 * @Date 2020/9/4
 * @Copyright 神州优车集团
 * @since 1.0
 */
public class Test {

    public static void main(String[] args) {
        final String jsonStr = IoUtil.read(FileUtil.getInputStream("C:\\Users\\Administrator\\Desktop\\a.json"), "utf-8");
        JSONObject json = JSONObject.parseObject(jsonStr);
        final HashMap<Integer, TreeNode> organ = Maps.newHashMap();
        putOrganIdAndName(json, organ);

        final LinkedList<TreeNode> treeNodes = Lists.newLinkedList();
        getAllNotes(organ, 20204236, treeNodes);
        System.out.println(treeNodes);
        printTree(treeNodes);
        treeNodes.clear();

        System.out.println("--------------------------------");

        getAllNotes(organ, 20204389, treeNodes);
        System.out.println(treeNodes);
        printTree(treeNodes);
    }

    public static void printTree(LinkedList<TreeNode> treeNodes) {
        int count = 0;
        Origin origin = new Origin();
        for (int i = treeNodes.size() - 1; i >= 0; i--) {
            final TreeNode treeNode = treeNodes.get(i);
            count++;
            if (count == 1) {
                origin.setFirst(treeNode.getName());
            } else if (count == 2) {
                origin.setSecond(treeNode.getName());
            } else if (count == 3) {
                origin.setThree(treeNode.getName());
            } else if (count == 4) {
                origin.setFour(treeNode.getName());
            }
        }
        System.out.println(origin);
    }

    /**
     * 根据组织结构id迭代出所有的父级组织名称
     *
     * @param organ
     * @param id
     * @param list
     */
    public static void getAllNotes(Map<Integer, TreeNode> organ, Integer id, List<TreeNode> list) {
        if (null == id) {
            return;
        }
        final TreeNode treeNode = organ.get(id);
        if (null != treeNode) {
            list.add(treeNode);
            getAllNotes(organ, treeNode.getParentId(), list);
        }
    }

    /**
     * 递归存放组织结构id及对应的组织结构名称
     *
     * @param jsonObject
     * @param organ
     */
    public static void putOrganIdAndName(JSONObject jsonObject, Map<Integer, TreeNode> organ) {
        final Integer id = jsonObject.getInteger("id");
        final String name = jsonObject.getString("name");
        final Integer parentId = jsonObject.getInteger("parentId");
        TreeNode treeNode = new TreeNode(id, name, parentId);
        organ.put(id, treeNode);
        final JSONArray children = jsonObject.getJSONArray("children");
        if (null != children && children.size() != 0) {
            for (int i = 0; i < children.size(); i++) {
                JSONObject object = children.getJSONObject(i);
                putOrganIdAndName(object, organ);
            }
        }
    }

    static class TreeNode {

        private Integer id;
        private String name;
        private Integer parentId;

        public TreeNode(Integer id, String name, Integer parentId) {
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
            return "TreeNode{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", parentId='" + parentId + '\'' +
                    '}';
        }
    }

    static class Origin {

        private String first;

        private String second;

        private String three;

        private String four;

        public String getFirst() {
            return first;
        }

        public void setFirst(String first) {
            this.first = first;
        }

        public String getSecond() {
            return second;
        }

        public void setSecond(String second) {
            this.second = second;
        }

        public String getThree() {
            return three;
        }

        public void setThree(String three) {
            this.three = three;
        }

        public String getFour() {
            return four;
        }

        public void setFour(String four) {
            this.four = four;
        }

        @Override
        public String toString() {
            return "Origin{" +
                    "first='" + first + '\'' +
                    ", second='" + second + '\'' +
                    ", three='" + three + '\'' +
                    ", four='" + four + '\'' +
                    '}';
        }
    }
}
