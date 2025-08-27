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

    /**
     * 生成以指定表为中心的子图（包含正向和逆向关系）
     * @param center 中心节点的表名
     * @return 包含子图节点和边信息的GraphDTO对象
     */
    @Transactional(readOnly = true)
    public GraphDTO subgraphFor(String center) {
        // 1. 从数据库获取所有边数据（包含关联的节点信息）
        List<EdgeEntity> all = edgeRepo.findAllWithNodes();

        // 2. 构建邻接表：创建两个映射来存储正向和逆向关系
        // 存储每个节点的出边（指向的节点）
        Map<String, Set<String>> out = new HashMap<>();
        // 存储每个节点的入边（指向该节点的节点）
        Map<String, Set<String>> in  = new HashMap<>();

        // 3. 遍历所有边，填充邻接表
        for (EdgeEntity e : all) {
            // 获取源节点名称
            String s = e.getSource().getName();
            // 获取目标节点名称
            String t = e.getTarget().getName();

            // 处理正向关系 (s -> t)
            if (!out.containsKey(s)) {
                // 如果源节点尚未记录，初始化其出边集合
                out.put(s, new LinkedHashSet<>());
            }
            // 将目标节点添加到源节点的出边集合中
            out.get(s).add(t);

            // 处理逆向关系 (t <- s)
            if (!in.containsKey(t)) {
                // 如果目标节点尚未记录，初始化其入边集合
                in.put(t, new LinkedHashSet<>());
            }
            // 将源节点添加到目标节点的入边集合中
            in.get(t).add(s);
        }

        // 4. 以center为核心进行双向BFS，探索指定深度内的所有相关节点
        // 存储需要保留的节点集合（按探索顺序）
        Set<String> keep = new LinkedHashSet<>();
        // BFS队列，用于按层探索节点
        Deque<String> q = new ArrayDeque<>();
        // 将中心节点添加到保留集合
        keep.add(center);
        // 将中心节点加入队列，作为BFS的起点
        q.add(center);

        // 5. 执行BFS：当队列不为空且未达到指定深度时继续探索
        while (!q.isEmpty()) {
            // 记录当前层的节点数量
            int size = q.size();
            // 遍历当前层的所有节点
            for (int i = 0; i < size; i++) {
                // 从队列中取出一个节点
                String u = q.poll();

                // 正向探索：遍历当前节点的所有出边（指向的节点）
                for (String v : out.getOrDefault(u, Set.of())) {
                    // 如果目标节点尚未在保留集合中
                    if (!keep.contains(v)) {
                        // 将其添加到保留集合
                        keep.add(v);
                        // 并将其加入队列，以便后续探索
                        q.add(v);
                    }
                }

                // 逆向探索：遍历当前节点的所有入边（指向该节点的节点）
                for (String v : in.getOrDefault(u, Set.of())) {
                    // 如果源节点尚未在保留集合中
                    if (!keep.contains(v)) {
                        // 将其添加到保留集合
                        keep.add(v);
                        // 并将其加入队列，以便后续探索
                        q.add(v);
                    }
                }
            }
        }

        // 6. 构建子图数据：根据BFS得到的节点集合，筛选相关的边和节点
        // 创建返回的DTO对象
        GraphDTO dto = new GraphDTO();
        // 记录已添加到DTO的节点，避免重复添加
        Set<String> added = new HashSet<>();

        // 7. 遍历所有边，筛选出至少一端在保留集合中的边
        for (EdgeEntity e : all) {
            // 获取边的源节点
            String s = e.getSource().getName();
            // 获取边的目标节点
            String t = e.getTarget().getName();

            // 跳过两端都不在保留集合中的边（与子图无关的边）
            if (!keep.contains(s) && !keep.contains(t)) {
                continue;
            }

            // 8. 添加源节点到DTO（如果尚未添加）
            if (!added.contains(s)) {
                // 创建节点对象：使用名称作为ID和标签，并包含节点类型信息
                dto.getNodes().add(new GraphDTO.Node(s, s, e.getSource().getType()));
                // 标记该节点已添加
                added.add(s);
            }

            // 9. 添加目标节点到DTO（如果尚未添加）
            if (!added.contains(t)) {
                dto.getNodes().add(new GraphDTO.Node(t, t, e.getTarget().getType()));
                // 标记该节点已添加
                added.add(t);
            }

            // 10. 添加边到DTO（无论节点是否新添加，边都需要添加）
            dto.getEdges().add(new GraphDTO.Edge(s, t));
        }

        // 11. 返回构建好的子图DTO
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
