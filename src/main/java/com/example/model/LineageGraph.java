package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter @Setter @NoArgsConstructor
public class LineageGraph {

    @Getter @Setter @AllArgsConstructor @NoArgsConstructor
    public static class Node {
        private String id;       // 唯一名（库.表 或 过程节点 id）
        private String label;    // 展示名
        private String type;     // table / view / process
    }

    @Getter @Setter @AllArgsConstructor @NoArgsConstructor
    public static class Edge {
        private String source;
        private String target;
        private Integer stmtIndex; // 用于“过程节点分组”
    }

    private final Map<String, Node> nodeMap = new LinkedHashMap<>();
    private final List<Edge> edges = new ArrayList<>();

    public void addNode(String id) {
        addNode(id, id, "table");
    }

    public void addNode(String id, String label, String type) {
        if (!nodeMap.containsKey(id)) {
            nodeMap.put(id, new Node(id, label, type));
        }
    }

    public void addEdge(String src, String tgt) {
        addEdge(src, tgt, null);
    }

    public void addEdge(String src, String tgt, Integer stmtIndex) {
        edges.add(new Edge(src, tgt, stmtIndex));
    }

    public List<Node> getNodes() {
        return new ArrayList<>(nodeMap.values());
    }

    public List<Edge> getEdges() {
        return edges;
    }
}
