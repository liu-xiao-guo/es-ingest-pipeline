package com.liuxg.plugin.ingest;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import com.liuxg.ingest.SampleProcessor;

import java.util.Collections;
import java.util.Map;

public class ingestPlugin extends Plugin implements IngestPlugin {
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(SampleProcessor.TYPE, new SampleProcessor.Factory());
    }
}