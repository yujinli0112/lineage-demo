package com.example.service;

import com.example.dao.EdgeRepo;
import com.example.dao.LineageRunRepo;
import com.example.dao.TableNodeRepo;
import com.example.dto.GraphDTO;
import com.example.dto.TableSummaryDTO;
import com.example.model.EdgeEntity;
import com.example.model.LineageGraph;
import com.example.model.LineageRunEntity;
import com.example.model.TableNodeEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@Service
public class PersistenceService {
    private final TableNodeRepo tableRepo;
    private final EdgeRepo edgeRepo;
    private final LineageRunRepo runRepo;

    public PersistenceService(TableNodeRepo tableRepo, EdgeRepo edgeRepo, LineageRunRepo runRepo) {
        this.tableRepo = tableRepo;
        this.edgeRepo = edgeRepo;
        this.runRepo = runRepo;
    }

    @Transactional
    public Long saveRun(String sql, LineageGraph graph, Map<Integer, String> stmtLabels) throws Exception {
        LineageRunEntity run = new LineageRunEntity();
        run.setSqlText(sql);
        run.setSqlHash(sha256(sql));
        run.setStmtCount(stmtLabels != null ? stmtLabels.size() : null);
        run = runRepo.save(run);

        Map<String, TableNodeEntity> nodeCache = new HashMap<>();
        for (LineageGraph.Node n : graph.getNodes()) {
            String name = n.getId();
            TableNodeEntity ent = tableRepo.findByName(name).orElse(null);
            if (ent == null) {
                ent = new TableNodeEntity();
                ent.setName(name);
                ent.setType(n.getType() == null ? "table" : n.getType());
                ent.setDisplayName(n.getLabel() == null ? name : n.getLabel());
                ent = tableRepo.save(ent);
            } else {
                ent = tableRepo.save(ent);
            }
            nodeCache.put(name, ent);
        }

        for (LineageGraph.Edge e : graph.getEdges()) {
            TableNodeEntity src = nodeCache.get(e.getSource());
            TableNodeEntity tgt = nodeCache.get(e.getTarget());
            if (src == null || tgt == null) {
                continue;
            }
            EdgeEntity ee = new EdgeEntity();
            ee.setSource(src);
            ee.setTarget(tgt);
            ee.setRun(run);
            ee.setStmtIndex(e.getStmtIndex());
            if (e.getStmtIndex() != null && stmtLabels != null) {
                ee.setStepLabel(stmtLabels.getOrDefault(e.getStmtIndex(), null));
            }
            edgeRepo.save(ee);
        }
        return run.getId();
    }

    @Transactional(readOnly = true)
    public List<TableSummaryDTO> listTables(String keyword) {
        List<TableNodeEntity> nodes = tableRepo.searchByKeyword(keyword);
        List<EdgeEntity> edges = edgeRepo.findAllWithNodes();
        Map<String, Long> inDeg = new HashMap<>();
        Map<String, Long> outDeg = new HashMap<>();
        for (EdgeEntity e : edges) {
            String s = e.getSource().getName();
            String t = e.getTarget().getName();
            outDeg.put(s, outDeg.getOrDefault(s, 0L) + 1L);
            inDeg.put(t, inDeg.getOrDefault(t, 0L) + 1L);
        }
        List<TableSummaryDTO> out = new ArrayList<>();
        for (TableNodeEntity n : nodes) {
            long in = inDeg.getOrDefault(n.getName(), 0L);
            long outd = outDeg.getOrDefault(n.getName(), 0L);
            out.add(new TableSummaryDTO(n.getName(), n.getType(), in, outd));
        }
        return out;
    }

    /** 以某个表为中心（默认 10 层，含逆向/正向）并插入“过程节点” */
    // PersistenceService.java
    @Transactional(readOnly = true)
    public GraphDTO subgraphFor(String center, int depth) {
        List<EdgeEntity> all = edgeRepo.findAllWithNodes();

        Map<String, Set<String>> out = new HashMap<>();
        Map<String, Set<String>> in  = new HashMap<>();
        for (EdgeEntity e : all) {
            String s = e.getSource().getName();
            String t = e.getTarget().getName();
            if (!out.containsKey(s)) { out.put(s, new LinkedHashSet<>()); }
            out.get(s).add(t);
            if (!in.containsKey(t)) { in.put(t, new LinkedHashSet<>()); }
            in.get(t).add(s);
        }

        // 以 center 为核心做双向 BFS（depth 层）
        Set<String> keep = new LinkedHashSet<>();
        Deque<String> q = new ArrayDeque<>();
        keep.add(center);
        q.add(center);
        int level = 0;
        while (!q.isEmpty() && level < depth) {
            int size = q.size();
            for (int i = 0; i < size; i++) {
                String u = q.poll();
                for (String v : out.getOrDefault(u, Set.of())) {
                    if (!keep.contains(v)) {
                        keep.add(v);
                        q.add(v);
                    }
                }
                for (String v : in.getOrDefault(u, Set.of())) {
                    if (!keep.contains(v)) {
                        keep.add(v);
                        q.add(v);
                    }
                }
            }
            level++;
        }

        // 只输出真实表/视图，直接连边；不再构造任何“过程/汇聚”节点
        GraphDTO dto = new GraphDTO();
        Set<String> added = new HashSet<>();
        for (EdgeEntity e : all) {
            String s = e.getSource().getName();
            String t = e.getTarget().getName();
            if (!keep.contains(s) && !keep.contains(t)) {
                continue;
            }
            if (!added.contains(s)) {
                dto.getNodes().add(new GraphDTO.Node(s, s, e.getSource().getType())); // table/view
                added.add(s);
            }
            if (!added.contains(t)) {
                dto.getNodes().add(new GraphDTO.Node(t, t, e.getTarget().getType())); // table/view
                added.add(t);
            }
            dto.getEdges().add(new GraphDTO.Edge(s, t));
        }
        return dto;
    }


    private String sha256(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] b = md.digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }
}
