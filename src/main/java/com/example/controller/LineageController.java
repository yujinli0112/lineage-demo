package com.example.controller;

import com.example.dto.GraphDTO;
import com.example.dto.TableSummaryDTO;
import com.example.service.LineageService;
import com.example.service.PersistenceService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class LineageController {

    private final LineageService lineageService;
    private final PersistenceService persistenceService;

    public LineageController(LineageService lineageService, PersistenceService persistenceService) {
        this.lineageService = lineageService;
        this.persistenceService = persistenceService;
    }

    public record SqlPayload(String sql) {}

    /** 解析 + 落库 */
    @PostMapping(path="/lineage/parse-save", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> parseAndSave(@RequestBody SqlPayload payload) throws Exception {
        var r = lineageService.buildAndSave(payload.sql());
        Map<String, Object> out = new HashMap<>();
        out.put("runId", r.runId);
        out.put("nodes", r.graph.getNodes());
        out.put("edges", r.graph.getEdges());
        return out;
    }

    /** 列出所有表（可带关键字） */
    @GetMapping(path="/tables", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<TableSummaryDTO> listTables(@RequestParam(name="kw", required = false) String kw) {
        return persistenceService.listTables(kw);
    }

    /** 获取以指定表为中心的子图（depth 默认 10） */
    @GetMapping(path="/graph", produces = MediaType.APPLICATION_JSON_VALUE)
    public GraphDTO graphFor(@RequestParam("center") String center,
                             @RequestParam(name="depth", required = false) Integer depth) {
        // 默认10，最多50，防爆图
        int d = (depth == null ? 10 : Math.max(1, Math.min(depth, 50)));
        return persistenceService.subgraphFor(center, d);
    }
}
