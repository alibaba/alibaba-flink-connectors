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

package com.alibaba.flink.connectors.cloudhbase.example;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.cloudhbase.sink.CloudHBaseRecordResolver;
import com.alibaba.flink.connectors.cloudhbase.sink.CloudHBaseSinkFunction;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Example for the usage of {@link CloudHBaseSinkFunction}.
 */
public class CloudHBaseSinkFunctionExample {

	public static void main(String[] args) {
		String zkQuorum = "sink.cloudhbase.connectors.flink.alibaba.com:2181";
		String tableName = "tableName";
		String columnFamily = "columnFamily";
		int numColumns = 3;
		Row columnNames = new Row(numColumns);
		columnNames.setField(0, "a");
		columnNames.setField(1, "b");
		columnNames.setField(2, "c");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(new RandomTupleSourceFunction(columnNames))
				.addSink(new CloudHBaseSinkFunction<>(
						zkQuorum, tableName, new TupleRecordResolver(columnFamily, columnNames)));
	}

	static class RandomTupleSourceFunction extends RichSourceFunction<Tuple3<Boolean, Long, Row>> {

		private final Row columnNames;

		private transient boolean isCancelled;

		RandomTupleSourceFunction(Row columnNames) {
			this.columnNames = columnNames;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			this.isCancelled = false;
		}

		@Override
		public void run(SourceContext<Tuple3<Boolean, Long, Row>> sourceContext) throws Exception {
			while (!isCancelled) {
				sourceContext.collect(ThreadLocalRandom.current().nextBoolean() ? getPutTuple() : getDeleteTuple());
			}
		}

		private Tuple3<Boolean, Long, Row> getPutTuple() {
			ThreadLocalRandom random = ThreadLocalRandom.current();
			long rowKey = random.nextLong();
			Row row = new Row(columnNames.getArity());
			for (int i = 0; i < columnNames.getArity(); i++) {
				row.setField(i, random.nextDouble());
			}
			return Tuple3.of(true, rowKey, row);
		}

		private Tuple3<Boolean, Long, Row> getDeleteTuple() {
			ThreadLocalRandom random = ThreadLocalRandom.current();
			long rowKey = random.nextLong();
			Row row = new Row(columnNames.getArity());
			for (int i = 0; i < columnNames.getArity(); i++) {
				if (random.nextBoolean()) {
					row.setField(i, true);
				}
			}
			return Tuple3.of(false, rowKey, row);
		}

		@Override
		public void close() throws Exception {
			super.close();
		}

		@Override
		public void cancel() {
			this.isCancelled = true;
		}
	}

	/**
	 * A resolver for only one column family.
	 */
	static class TupleRecordResolver implements CloudHBaseRecordResolver<Tuple3<Boolean, Long, Row>> {

		private final String columnFamily;

		private final Row columnNames;

		public TupleRecordResolver(String columnFamily, Row columnNames) {
			this.columnFamily = columnFamily;
			this.columnNames = columnNames;
		}

		@Override
		public String getRowKey(Tuple3<Boolean, Long, Row> record) {
			return record.f1.toString();
		}

		@Override
		public Mutation getMutation(Tuple3<Boolean, Long, Row> record) {
			if (record.f0) {
				// Put mutation
				Put put = new Put(record.f1.toString().getBytes());
				Row row = record.f2;
				for (int i = 0; i < columnNames.getArity(); i++) {
					Object object = row.getField(i);
					if (object != null) {
						put.addColumn(columnFamily.getBytes(),
								columnNames.getField(i).toString().getBytes(),
								object.toString().getBytes());
					}
				}
				return put;
			} else {
				// Delete mutation
				// Put mutation
				Delete delete = new Delete(record.f1.toString().getBytes());
				Row row = record.f2;
				for (int i = 0; i < columnNames.getArity(); i++) {
					Object object = row.getField(i);
					if (object != null) {
						delete.addColumn(columnFamily.getBytes(),
								columnNames.getField(i).toString().getBytes());
					}
				}
				return delete;
			}
		}

		@Override
		public void open() {

		}

		@Override
		public void close() {

		}
	}
}
