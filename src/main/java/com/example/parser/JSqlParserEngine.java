package com.example.parser;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;

import java.util.*;

public class JSqlParserEngine {
    private final SqlPreprocessor preprocessor;
    private final SourceTableFinder sourceFinder = new SourceTableFinder();
    private final TokenScannerFallback fallback = new TokenScannerFallback();

    public JSqlParserEngine(SqlPreprocessor preprocessor) {
        this.preprocessor = preprocessor;
    }

    public List<Statement> parseStatements(String sql) throws Exception {
        String pre = preprocessor == null ? sql : preprocessor.preprocess(sql);
        Statements statements = CCJSqlParserUtil.parseStatements(pre);
        return statements.getStatements();
    }

    public Optional<String> extractTargetTable(Statement stmt) {
        if (stmt instanceof Insert ins) {
            Table t = ins.getTable();
            if (t != null) {
                return Optional.of(normalize(t.getFullyQualifiedName()));
            }
        }
        if (stmt instanceof CreateTable ct) {
            Select sel = ct.getSelect();
            if (sel != null && ct.getTable() != null) {
                return Optional.of(normalize(ct.getTable().getFullyQualifiedName()));
            }
        }
        // 其他类型（如 CREATE VIEW）走兜底
        return Optional.empty();
    }

    public Set<String> extractSourceTables(Statement stmt) {
        Set<String> src = new LinkedHashSet<>();
        if (stmt instanceof Insert ins) {
            Select sel = ins.getSelect();
            if (sel != null) {
                src.addAll(sourceFinder.getSourceTables(sel));
            }
        } else if (stmt instanceof CreateTable ct && ct.getSelect() != null) {
            src.addAll(sourceFinder.getSourceTables(ct.getSelect()));
        } else if (stmt instanceof Select sel) {
            src.addAll(sourceFinder.getSourceTables(sel));
        }
        return src;
    }

    public FallbackResult fallbackExtract(String sql) {
        return fallback.extract(sql);
    }

    private static String normalize(String name) {
        if (name == null) {
            return null;
        }
        String n = name.trim();
        if (n.startsWith("`") && n.endsWith("`")) {
            n = n.substring(1, n.length() - 1);
        }
        if (n.startsWith("\"") && n.endsWith("\"")) {
            n = n.substring(1, n.length() - 1);
        }
        n = n.replace("`.", ".").replace(".`", ".").replace("\".", ".").replace(".\"", ".");
        return n.toLowerCase(Locale.ROOT);
    }

    /** 兜底结果：支持多个目标表（INSERT ALL / Hive 多 INSERT / CREATE VIEW） */
    public static class FallbackResult {
        public final Set<String> targets;
        public final Set<String> sources;
        public FallbackResult(Set<String> targets, Set<String> sources) {
            this.targets = (targets == null ? Set.of() : targets);
            this.sources = (sources == null ? Set.of() : sources);
        }
    }
}
