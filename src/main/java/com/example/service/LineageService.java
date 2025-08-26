package com.example.service;

import com.example.model.LineageGraph;
import com.example.parser.JSqlParserEngine;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class LineageService {

    private final JSqlParserEngine engine = new JSqlParserEngine(new com.example.parser.SqlPreprocessor(true));
    private final PersistenceService persistence;

    public LineageService(PersistenceService persistence) {
        this.persistence = persistence;
    }

    public static class ParseResult {
        public final LineageGraph graph;
        public final Long runId;
        public ParseResult(LineageGraph g, Long id) { this.graph = g; this.runId = id; }
    }

    /** 解析 + 落库 + 返回 runId 与图 */
    public ParseResult buildAndSave(String sql) throws Exception {
        LineageGraph g = new LineageGraph();
        Map<Integer, String> stmtLabels = new LinkedHashMap<>();

        List<Statement> stmts;
        try {
            stmts = engine.parseStatements(sql);
        } catch (Exception e) {
            // 全文失败，走兜底（多目标）
            var fb = engine.fallbackExtract(sql);
            int idx = 1;
            if (!fb.targets.isEmpty()) {
                stmtLabels.put(idx, "INSERT-SELECT-" + idx);
                for (String tgt : fb.targets) {
                    g.addNode(tgt);
                    for (String src : fb.sources) {
                        if (!tgt.equals(src)) {
                            g.addNode(src);
                            g.addEdge(src, tgt, idx);
                        }
                    }
                }
            } else {
                // 可能是 CREATE VIEW 等场景
                for (String src : fb.sources) {
                    g.addNode(src);
                }
            }
            Long runId = persistence.saveRun(sql, g, stmtLabels);
            return new ParseResult(g, runId);
        }

        int stmtIndex = 0;
        for (Statement s : stmts) {
            // 先尝试 AST
            var targetOpt = engine.extractTargetTable(s);
            var sources = engine.extractSourceTables(s);

            if (targetOpt.isPresent()) {
                stmtIndex++;
                String tgt = targetOpt.get();
                g.addNode(tgt);

                if (s instanceof Insert) {
                    stmtLabels.put(stmtIndex, "INSERT-SELECT-" + stmtIndex);
                } else if (s instanceof CreateTable) {
                    stmtLabels.put(stmtIndex, "CTAS-" + stmtIndex);
                } else {
                    stmtLabels.put(stmtIndex, "STEP-" + stmtIndex);
                }

                if (sources.isEmpty()) {
                    var fb = engine.fallbackExtract(s.toString());
                    sources.addAll(fb.sources);
                }
                for (String src : sources) {
                    if (!tgt.equals(src)) {
                        g.addNode(src);
                        g.addEdge(src, tgt, stmtIndex);
                    }
                }
            } else {
                // 该语句可能是 CREATE VIEW 或 Hive 多 INSERT 块
                var fb = engine.fallbackExtract(s.toString());
                if (!fb.targets.isEmpty()) {
                    stmtIndex++;
                    stmtLabels.put(stmtIndex, "INSERT-SELECT-" + stmtIndex);
                    for (String tgt : fb.targets) {
                        g.addNode(tgt);
                        for (String src : fb.sources) {
                            if (!tgt.equals(src)) {
                                g.addNode(src);
                                g.addEdge(src, tgt, stmtIndex);
                            }
                        }
                    }
                } else {
                    for (String src : sources) {
                        g.addNode(src);
                    }
                }
            }
        }

        Long runId = persistence.saveRun(sql, g, stmtLabels);
        return new ParseResult(g, runId);
    }
}
