// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import com.google.api.services.bigquery.model.TableRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class FlattenJson {



private static final Logger LOG = LoggerFactory.getLogger(FlattenJson.class);



public interface FlattenJsonOptions extends PipelineOptions {
@Description("The Google Cloud project ID")
@Required
String getProject();
void setProject(String value);



@Description("The region where the Dataflow job will run")
@Required
String getRegion();
void setRegion(String value);



@Description("A Cloud Storage location for temporary files")
@Required
String getTempLocation();
void setTempLocation(String value);



@Description("The subnetwork to use for the Dataflow workers")
String getSubnetwork();
void setSubnetwork(String value);



@Description("The Cloud Pub/Sub subscription to read from")
@Required
String getInputSubscription();
void setInputSubscription(String value);



@Description("The BigQuery table to write to")
@Required
String getOutputTable();
void setOutputTable(String value);



@Description("The BigQuery table for deadletter records")
@Required
String getDeadletterTable();
void setDeadletterTable(String value);



@Description("The BigQuery dataset ID")
@Required
String getDatasetId();
void setDatasetId(String value);
}



public static void main(String[] args) {
FlattenJsonOptions options = PipelineOptionsFactory.fromArgs(args)
.withValidation()
.as(FlattenJsonOptions.class);
Pipeline pipeline = Pipeline.create(options);



final TupleTag<TableRow> mainOutputTag = new TupleTag<TableRow>(){};
final TupleTag<TableRow> deadletterTag = new TupleTag<TableRow>(){};



PCollectionTuple outputs = pipeline
.apply("Read from Pub/Sub", PubsubIO.readStrings()
.fromSubscription(options.getInputSubscription()))
.apply("Flatten JSON", ParDo.of(new FlattenJsonFn(mainOutputTag, deadletterTag))
.withOutputTags(mainOutputTag, TupleTagList.of(deadletterTag)));



outputs.get(mainOutputTag)
.apply("Write to BigQuery", BigQueryIO.writeTableRows()
.to(options.getOutputTable())
.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));



outputs.get(deadletterTag)
.apply("Write to Deadletter", BigQueryIO.writeTableRows()
.to(options.getDeadletterTable())
.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));



pipeline.run();
}



static class FlattenJsonFn extends DoFn<String, TableRow> {
private final TupleTag<TableRow> mainOutputTag;
private final TupleTag<TableRow> deadletterTag;
private transient ObjectMapper mapper;



FlattenJsonFn(TupleTag<TableRow> mainOutputTag, TupleTag<TableRow> deadletterTag) {
this.mainOutputTag = mainOutputTag;
this.deadletterTag = deadletterTag;
}



@Setup
public void setup() {
mapper = new ObjectMapper();
}



@ProcessElement
public void processElement(@Element String json, MultiOutputReceiver out) {
try {
JsonNode root = mapper.readTree(json);



String id = root.path("id").asText();
String idType = root.path("idType").asText();
String experimentationId = root.path("experimentationId").asText();
String xGrid = root.path("x-grid").asText();



JsonNode zoneArray = root.path("zone");
if (zoneArray.isArray()) {
for (JsonNode zoneNode : zoneArray) {
int zoneId = zoneNode.path("zoneId").asInt();
int zoneRank = zoneNode.path("zoneRank").asInt();



JsonNode nbaList = zoneNode.path("nbaList");
if (nbaList.isArray()) {
for (JsonNode nbaNode : nbaList) {
int nbaCardId = nbaNode.path("nbaCardId").asInt();
int nbaRank = nbaNode.path("nbaRank").asInt();



TableRow row = new TableRow()
.set("id", id)
.set("idType", idType)
.set("experimentationId", experimentationId)
.set("zoneId", zoneId)
.set("zoneRank", zoneRank)
.set("nbaCardId", nbaCardId)
.set("nbaRank", nbaRank)
.set("xGrid", xGrid);



out.get(mainOutputTag).output(row);
}
} else {
LOG.warn("nbaList is not an array in zone: {}", zoneNode.toString());
outputToDeadletter(out, json, "nbaList is not an array");
}
}
} else {
LOG.warn("zone is not an array in input JSON: {}", json);
outputToDeadletter(out, json, "zone is not an array");
}
} catch (Exception e) {
LOG.error("Error processing JSON: " + json, e);
outputToDeadletter(out, json, "Error processing JSON: " + e.getMessage());
}
}



private void outputToDeadletter(MultiOutputReceiver out, String originalJson, String errorMessage) {
TableRow deadLetterRow = new TableRow()
.set("error", errorMessage)
.set("raw_data", originalJson);
out.get(deadletterTag).output(deadLetterRow);
}
}
}

/* Input json



{
"id": "377767901",
"idType": "RETAIL_PROFILE_ID_TYPE",
"experimentationId": "mlp",
"zone": [
{
"zoneId": 1,
"zoneRank": 1,
"nbaList": [
{
"nbaCardId": 11,
"nbaRank": 1
}
]
},
{
"zoneId": 2,
"zoneRank": 2,
"nbaList": [
{
"nbaCardId": 21,
"nbaRank": 1
}
]
},
{
"zoneId": 3,
"zoneRank": 3,
"nbaList": [
{
"nbaCardId": 31,
"nbaRank": 1
}
]
}
],
"x-grid": "bcki3jckjb2k"
}

*/
