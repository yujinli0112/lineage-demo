package com.example.model;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * @author yujin
 * @date 2025/8/25 14:50
 **/
public class LineageEdge {
    @JsonProperty("source")
    private final String source;
    @JsonProperty("target")
    private final String target;


    public LineageEdge(String source, String target) {
        this.source = source; this.target = target;
    }
    public String getSource() { return source; }
    public String getTarget() { return target; }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LineageEdge)) return false;
        LineageEdge that = (LineageEdge) o;
        return Objects.equals(source, that.source) && Objects.equals(target, that.target);
    }
    @Override public int hashCode() { return Objects.hash(source, target); }
}
