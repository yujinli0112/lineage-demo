package com.example.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter @Setter @NoArgsConstructor
public class GraphDTO {
    @Getter @Setter @AllArgsConstructor @NoArgsConstructor
    public static class Node { public String id; public String label; public String type; }
    @Getter @Setter @AllArgsConstructor @NoArgsConstructor
    public static class Edge { public String source; public String target; }

    public List<Node> nodes = new ArrayList<>();
    public List<Edge> edges = new ArrayList<>();
}
