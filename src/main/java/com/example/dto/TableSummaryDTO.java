package com.example.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @AllArgsConstructor @NoArgsConstructor
public class TableSummaryDTO {
    public String name;
    public String type;
    public long inDegree;
    public long outDegree;
}
