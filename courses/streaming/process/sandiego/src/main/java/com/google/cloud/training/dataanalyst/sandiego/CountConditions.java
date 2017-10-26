/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.training.dataanalyst.sandiego;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A dataflow pipeline that counts LineInfo objects and writes to BigQuery
 *
 * @author woutscheepers
 */
@SuppressWarnings("serial")
public class CountConditions {

    public static interface MyOptions extends DataflowPipelineOptions {

    }

    private static final Logger LOG = LoggerFactory.getLogger(CountConditions.class);

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        //TODO: set the streaming option to false
        options.setStreaming(true);
        Pipeline p = Pipeline.create(options);

        String countConditionsTable = options.getProject() + ":demos.count_conditions";
        //TODO: change the dataset path to point to the file in your bucket
        String datasetPath = "gs://my-bucket/dataset.csv";

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        PCollection<LaneInfo> currentConditions = p
                //TODO change PubsubIO to TextIO
                .apply("GetMessages", PubsubIO.readStrings().fromTopic(topic))
                .apply("ExtractData", ParDo.of(new DoFn<String, LaneInfo>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        String line = c.element();
                        c.output(LaneInfo.newLaneInfo(line));
                    }
                }));

        //TODO: Add a PColletion with the sums:
        //PCollection<Long> sums = ...


        //convert the sums PCollection to a PCollection of TableRows:
        sums.apply("ToBQRow", ParDo.of(new DoFn<Long, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                TableRow row = new TableRow();
                row.set("count", c.element());
                c.output(row);
            }
        }))
                .apply(BigQueryIO.writeTableRows().to(countConditionsTable)//
                        .withSchema(schema)//
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }
}
