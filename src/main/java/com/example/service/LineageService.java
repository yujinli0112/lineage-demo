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
        public final boolean saved;
        public final String message;
        public ParseResult(LineageGraph g, Long id, boolean saved, String message) {
            this.graph = g;
            this.runId = id;
            this.saved = saved;
            this.message = message;
        }
    }

    /**
     * 解析SQL语句并构建血缘图，然后将结果保存到数据库
     *
     * @param sql 要解析的SQL语句
     * @return 包含血缘图和执行ID的解析结果
     * @throws Exception 解析过程中可能出现的异常
     */
    public ParseResult buildAndSave(String sql) throws Exception {
        // 初始化空的血缘图
        LineageGraph g = new LineageGraph();
        // 存储每个SQL语句的标签（用于标识语句类型和顺序）
        Map<Integer, String> stmtLabels = new LinkedHashMap<>();

        List<Statement> stmts;
        try {
            // 尝试使用SQL解析引擎解析SQL语句
            stmts = engine.parseStatements(sql);
        } catch (Exception e) {
            // 如果标准解析失败，使用降级/兜底解析方法
            var fb = engine.fallbackExtract(sql);
            int idx = 1;

            // 处理降级解析结果：如果有目标表，构建简单血缘关系
            if (!fb.targets.isEmpty()) {
                stmtLabels.put(idx, "INSERT-SELECT-" + idx);
                for (String tgt : fb.targets) {
                    // 添加目标表节点
                    g.addNode(tgt);
                    for (String src : fb.sources) {
                        if (!tgt.equals(src)) {
                            // 添加源表节点
                            g.addNode(src);
                            // 添加源到目标的边
                            g.addEdge(src, tgt, idx);
                        }
                    }
                }
            } else {
                // 如果没有目标表（如CREATE VIEW语句），只添加源表
                for (String src : fb.sources) {
                    g.addNode(src);
                }
            }

            // 没有任何边 -> 不入库，友好返回
            if (g.getEdges().isEmpty()) {
                return new ParseResult(g, null, false, "已解析：未检测到写入目标（仅 SELECT），因此未入库。");
            }

            // 保存解析运行记录到数据库
            Long runId = persistence.saveRun(sql, g, stmtLabels);
            return new ParseResult(g, runId, true, "已解析并入库。");
        }

        // 正常解析路径：遍历每个SQL语句
        int stmtIndex = 0;
        for (Statement s : stmts) {
            // 尝试从AST中提取目标表和源表
            var targetOpt = engine.extractTargetTable(s);
            var sources = engine.extractSourceTables(s);

            // 如果成功提取到目标表
            if (targetOpt.isPresent()) {
                stmtIndex++;
                String tgt = targetOpt.get();
                // 添加目标表节点
                g.addNode(tgt);

                // 根据语句类型设置标签
                if (s instanceof Insert) {
                    stmtLabels.put(stmtIndex, "INSERT-SELECT-" + stmtIndex);
                } else if (s instanceof CreateTable) {
                    // CTAS = Create Table As Select
                    stmtLabels.put(stmtIndex, "CTAS-" + stmtIndex);
                } else {
                    stmtLabels.put(stmtIndex, "STEP-" + stmtIndex);
                }

                // 如果AST解析未能提取到源表，使用降级方法
                if (sources.isEmpty()) {
                    var fb = engine.fallbackExtract(s.toString());
                    sources.addAll(fb.sources);
                }

                // 为每个源表添加节点和边
                for (String src : sources) {
                    // 避免自环
                    if (!tgt.equals(src)) {
                        g.addNode(src);
                        g.addEdge(src, tgt, stmtIndex);
                    }
                }
            } else {
                // 该语句可能是不支持提取目标表的类型（如CREATE VIEW或Hive多INSERT）
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
                    // 如果没有目标表，只添加源表
                    for (String src : sources) {
                        g.addNode(src);
                    }
                }
            }
        }
        // 最终没有任何边 -> 不入库
        if (g.getEdges().isEmpty()) {
            return new ParseResult(g, null, false, "已解析：未检测到写入目标（仅 SELECT），因此未入库。");
        }

        Long runId = persistence.saveRun(sql, g, stmtLabels);
        return new ParseResult(g, runId, true, "已解析并入库。");
    }
}
