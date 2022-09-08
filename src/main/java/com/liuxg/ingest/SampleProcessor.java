package com.liuxg.ingest;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Locale;
import java.util.Map;

public final class SampleProcessor extends AbstractProcessor {

    public static final String TYPE = "sample";

    private final String field;
    private final String targetField;
    private final String defaultValue;
    private final boolean ignoreMissing;

    public SampleProcessor(String tag, String description, String field, String targetField, boolean ignoreMissing, String defaultValue) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.defaultValue = defaultValue;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    String getDefaultField() {
        return defaultValue;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        if (!document.hasField(field, true)) {
            if (ignoreMissing) {
                return document;
            } else {
                throw new IllegalArgumentException("field [" + field + "] not present as part of path [" + field + "]");
            }
        }
        // We fail here if the target field point to an array slot that is out of range.
        // If we didn't do this then we would fail if we set the value in the target_field
        // and then on failure processors would not see that value we tried to rename as we already
        // removed it.
        if (document.hasField(targetField, true)) {
            throw new IllegalArgumentException("field [" + targetField + "] already exists");
        }

        Object value = document.getFieldValue(field, Object.class);
        if( value!=null && value instanceof String ) {
            String myValue=value.toString().trim();
            if(myValue.length()>1){
                try {
                    document.setFieldValue(targetField, myValue.substring(0,1).toLowerCase(Locale.getDefault()));
                } catch (Exception e) {
                    // setting the value back to the original field shouldn't as we just fetched it from that field:
                    document.setFieldValue(field, value);
                    throw e;
                }
            }
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag,
                    config, "target_field");
            String defaultValue = ConfigurationUtils.readOptionalStringProperty(TYPE, tag,
                    config, "defaultValue");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag,
                    config, "ignore_missing", false);
            return new SampleProcessor(tag, description, field, targetField, ignoreMissing, defaultValue);
        }
    }
}