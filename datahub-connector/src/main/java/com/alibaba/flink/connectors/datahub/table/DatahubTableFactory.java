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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_ACCESS_ID;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_ACCESS_KEY;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_BATCH_SIZE;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_BUFFER_SIZE;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_ENDPOINT;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_MAX_RETRY_TIMES;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_PROJECT;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_RETRY_TIMEOUT_IN_MILLS;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_TOPIC;
import static com.alibaba.flink.connectors.datahub.table.DatahubDescriptorValidator.CONNECTOR_TYPE_VALUE_DATAHUB;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating Datahub table source and sink.
 */
@Internal
public class DatahubTableFactory implements TableSinkFactory<Row> {

	public String getConnectorTypeValue() {
		return CONNECTOR_TYPE_VALUE_DATAHUB;
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, getConnectorTypeValue()); // datahub
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_PROJECT);
		properties.add(CONNECTOR_TOPIC);
		properties.add(CONNECTOR_ACCESS_ID);
		properties.add(CONNECTOR_ACCESS_KEY);
		properties.add(CONNECTOR_ENDPOINT);

		properties.add(CONNECTOR_BATCH_SIZE);
		properties.add(CONNECTOR_BUFFER_SIZE);
		properties.add(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS);
		properties.add(CONNECTOR_RETRY_TIMEOUT_IN_MILLS);
		properties.add(CONNECTOR_MAX_RETRY_TIMES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	@Override
	public TableSink<Row> createTableSink(Map<String, String> prop) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(prop);

		new DatahubDescriptorValidator().validate(params);

		TableSchema schema = params.getTableSchema(SCHEMA);

		String project = params.getString(CONNECTOR_PROJECT);
		String topic = params.getString(CONNECTOR_TOPIC);
		String accessId = params.getString(CONNECTOR_ACCESS_ID);
		String accessKey = params.getString(CONNECTOR_ACCESS_KEY);
		String endpoint = params.getString(CONNECTOR_ENDPOINT);

		return new DatahubTableSink(
				project,
				topic,
				accessId,
				accessKey,
				endpoint,
				schema,
				params
		);
	}
}
