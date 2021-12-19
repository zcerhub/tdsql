package com.source;

import java.util.List;

public class Source {

    private String sourceName;
    private SourceDataBase sourceDataBase;

    public Source(String sourceName, SourceDataBase sourceDataBase) {
        this.sourceName = sourceName;
        this.sourceDataBase = sourceDataBase;
    }


    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public SourceDataBase getSourceDataBase() {
        return sourceDataBase;
    }

    public void setSourceDataBase(SourceDataBase sourceDataBase) {
        this.sourceDataBase = sourceDataBase;
    }
}
