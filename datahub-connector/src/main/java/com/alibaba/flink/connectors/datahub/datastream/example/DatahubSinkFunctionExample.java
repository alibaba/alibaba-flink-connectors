/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.datahub.datastream.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.flink.connectors.datahub.datastream.sink.DatahubRecordResolver;
import com.alibaba.flink.connectors.datahub.datastream.sink.DatahubSinkFunction;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordData;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import java.io.Serializable;

/**
 * Example to show how to use DatahubSinkFunction and RecordConverter.
 */
public class DatahubSinkFunctionExample implements Serializable {
	private String endPoint = "";
	private String projectName = "";
	private String topicName = "";
	private String accessId = "";
	private String accessKey = "";

	public void useDefaultRecordConverter() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.generateSequence(0, 100)
			.map((MapFunction<Long, RecordEntry>) aLong -> getRecordEntry(aLong, "default:"))
			.addSink(new DatahubSinkFunction<>(endPoint, projectName, topicName, accessId, accessKey,
											DatahubRecordResolver.NOOP_DATAHUB_RECORD_RESOLVER));
		env.execute();
	}

	public void useCustomRecordConverter() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DatahubRecordResolver<Long> recordResolver = message -> getRecordEntry(message, "custom:");
		env.generateSequence(0, 100).addSink(new DatahubSinkFunction<>(endPoint, projectName, topicName, accessId,
																	accessKey, recordResolver));
		env.execute();
	}

	private RecordEntry getRecordEntry(Long message, String s) {
		RecordSchema recordSchema = new RecordSchema();
		recordSchema.addField(new Field("type", FieldType.STRING));
		recordSchema.addField(new Field("value", FieldType.BIGINT));
		RecordEntry recordEntry = new RecordEntry();
		RecordData recordData = new TupleRecordData(recordSchema);
		((TupleRecordData) recordData).setField(0, s + message);
		((TupleRecordData) recordData).setField(1, message);
		recordEntry.setRecordData(recordData);
		return recordEntry;
	}

	public static void main(String[] args) throws Exception {
		DatahubSinkFunctionExample sinkFunctionExample = new DatahubSinkFunctionExample();
		sinkFunctionExample.useDefaultRecordConverter();
		sinkFunctionExample.useCustomRecordConverter();
	}
}
