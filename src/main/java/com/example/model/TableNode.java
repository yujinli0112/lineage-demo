package com.example.model;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * @author yujin
 * @date 2025/8/25 14:50
 **/
public class TableNode {
    @JsonProperty("id")
    private final String id; // 规范化后的库.表（或仅表）


    public TableNode(String id) { this.id = id; }
    public String getId() { return id; }


    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableNode)) {
            return false;
        }
        TableNode that = (TableNode) o;
        return Objects.equals(id, that.id);
    }


    @Override public int hashCode() { return Objects.hash(id); }
}
