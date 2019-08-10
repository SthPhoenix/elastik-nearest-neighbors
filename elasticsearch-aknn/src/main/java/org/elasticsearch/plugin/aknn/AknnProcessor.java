/*
 * Copyright [2019] [SthPhoenix]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.elasticsearch.plugin.aknn;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
//import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import static java.lang.Math.*;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readMap;


public class AknnProcessor extends AbstractProcessor {

    public static final String TYPE = "aknn";
    private final String field;
    private final String targetField;
    private final Map<String, Object> model;
    private final boolean ignoreMissing;
    private final LshModel lshModel;

    public static LshModel InitLsh(Map<String, Object> model) {
        LshModel lshModel;
        lshModel = LshModel.fromMap(model);
        return lshModel;
    }


    public AknnProcessor(String tag, String field, String targetField, Map<String, Object> model, boolean ignoreMissing, LshModel lshModel)
            throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.model = model;
        this.ignoreMissing = ignoreMissing;
        this.lshModel = lshModel;

    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {

        List<Double> vector;

        try {
            vector = this.castToListOfDouble(ingestDocument.getFieldValue(field, List.class));
        } catch (IllegalArgumentException e) {
            if (ignoreMissing) {
                return ingestDocument;
            }
            throw e;
        }

        ingestDocument.setFieldValue(targetField, lshModel.getVectorHashes(vector));

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public AknnProcessor create(Map<String, Processor.Factory> factories, String tag, Map<String, Object> config)
                throws Exception {
            String field = readStringProperty(TYPE, tag, config, "field");
            String targetField = readStringProperty(TYPE, tag, config, "target_field");
            Map<String, Object> model = readMap(TYPE,tag, config,"model");
            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            LshModel lshModel = InitLsh(model);

            return new AknnProcessor(tag, field, targetField, model, ignoreMissing, lshModel);
        }
    }



    private List<Double> castToListOfDouble(Object obj) {
        return ((List<?>) obj)
                .stream()
                .map(elm -> elm instanceof Integer ? new Double((Integer) elm) : (Double) elm)
                .collect(Collectors.toList());
    }



}
