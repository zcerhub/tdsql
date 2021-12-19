package com.source;

import java.util.List;

public class SourceDataBase {

    private String dataBaseName;
    private List<String> sourceTables;

    public SourceDataBase(String dataBaseName, List<String> sourceTables) {
        this.dataBaseName = dataBaseName;
        this.sourceTables = sourceTables;
    }

    public String getDataBaseName() {
        return dataBaseName;
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public List<String> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(List<String> sourceTables) {
        this.sourceTables = sourceTables;
    }
}
