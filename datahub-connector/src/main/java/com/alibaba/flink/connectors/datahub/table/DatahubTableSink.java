/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.datahub.table;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.datahub.datastream.sink.DatahubOutputFormat;

import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_BATCH_SIZE;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_BUFFER_SIZE;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_MAX_RETRY_TIMES;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_RETRY_TIMEOUT_IN_MILLS;

/**
 * Table Sink for Datahub.
 */
public class DatahubTableSink extends OutputFormatTableSink<Row> {

	private final String project;
	private final String topic;
	private final String accessId;
	private final String accessKey;
	private final String endpoint;
	private final TableSchema schema;
	private final DescriptorProperties  prop;

	public DatahubTableSink(
			String project,
			String topic,
			String accessId,
			String accessKey,
			String endpoint,
			TableSchema schema,
			DescriptorProperties prop) {
		this.project = project;
		this.topic = topic;
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.schema = schema;
		this.prop = prop;
	}

	@Override
	public DataType getConsumedDataType() {
		return schema.toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
		return new DatahubTableSink(project, topic, accessId, accessKey, endpoint, schema, prop);
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		RowTypeInfo flinkRowTypeInfo = new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
		DatahubOutputFormat outputFormat = new DatahubOutputFormat<Row>(
				endpoint,
				project,
				topic,
				accessId,
				accessKey,
				flinkRowTypeInfo);

		if (prop.containsKey(CONNECTOR_BUFFER_SIZE)) {
			outputFormat.setBufferSize(prop.getInt(CONNECTOR_BUFFER_SIZE));
		}

		if (prop.containsKey(CONNECTOR_BATCH_SIZE)) {
			outputFormat.setBatchSize(prop.getInt(CONNECTOR_BATCH_SIZE));
		}

		if (prop.containsKey(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS)) {
			outputFormat.setBatchWriteTimeout(prop.getLong(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS));
		}
		if (prop.containsKey(CONNECTOR_RETRY_TIMEOUT_IN_MILLS)) {
			outputFormat.setRetryTimeoutInMills(prop.getInt(CONNECTOR_RETRY_TIMEOUT_IN_MILLS));
		}

		if (prop.containsKey(CONNECTOR_MAX_RETRY_TIMES)) {
			outputFormat.setMaxRetryTimes(prop.getInt(CONNECTOR_MAX_RETRY_TIMES));
		}

		outputFormat.setRecordResolver(
				new DatahubRowRecordResolver(flinkRowTypeInfo, project, topic, accessId, accessKey, endpoint));

		return outputFormat;
	}
}
