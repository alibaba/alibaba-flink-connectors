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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.flink.connectors.datahub.datastream.source.DatahubSourceFunction;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;

import java.io.Serializable;
import java.util.List;

/**
 * Example to show how to use DatahubSourceFunction and RecordConverter.
 */
public class DatahubSourceFunctionExample implements Serializable {
	private String endPoint = "";
	private String projectName = "";
	private String topicName = "";
	private String accessId = "";
	private String accessKey = "";

	public void runExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DatahubSourceFunction datahubSource =
				new DatahubSourceFunction(endPoint, projectName, topicName, accessId, accessKey, 0,
										Long.MAX_VALUE, 1, 1, 1);
		env.addSource(datahubSource).flatMap(
				(FlatMapFunction<List<RecordEntry>, Tuple2<String, Long>>) (recordEntries, collector) -> {
			for (RecordEntry recordEntry : recordEntries) {
				collector.collect(getStringLongTuple2(recordEntry));
			}
		}).returns(new TypeHint<Tuple2<String, Long>>() {}).print();
		env.execute();
	}

	private Tuple2<String, Long> getStringLongTuple2(RecordEntry recordEntry) {
		Tuple2<String, Long> tuple2 = new Tuple2<>();
		TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
		tuple2.f0 = (String) recordData.getField(0);
		tuple2.f1 = (Long) recordData.getField(1);
		return tuple2;
	}

	public static void main(String[] args) throws Exception {
		DatahubSourceFunctionExample sourceFunctionExample = new DatahubSourceFunctionExample();
		sourceFunctionExample.runExample();
	}
}
