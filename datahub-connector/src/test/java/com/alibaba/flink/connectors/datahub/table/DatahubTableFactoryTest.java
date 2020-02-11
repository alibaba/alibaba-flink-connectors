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

import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;

import org.junit.Test;

import java.util.HashMap;
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
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link DatahubTableFactory}.
 */
public class DatahubTableFactoryTest {
	@Test
	public void testRequiredProperties() {
		Map<String, String> properties = getBasicProperties();

		final TableSink<?> actual = TableFactoryService.find(TableSinkFactory.class, properties)
				.createTableSink(properties);

		assertTrue(actual instanceof DatahubTableSink);
	}

	@Test
	public void testSupportedProperties() {
		Map<String, String> properties = getBasicProperties();

		properties.put(CONNECTOR_BATCH_SIZE, "1");
		properties.put(CONNECTOR_BUFFER_SIZE, "1");
		properties.put(CONNECTOR_RETRY_TIMEOUT_IN_MILLS, "3");
		properties.put(CONNECTOR_MAX_RETRY_TIMES, "10");
		properties.put(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS, "5");

		final TableSink<?> actual = TableFactoryService.find(TableSinkFactory.class, properties)
				.createTableSink(properties);

		assertTrue(actual instanceof DatahubTableSink);
	}

	private Map<String, String> getBasicProperties() {
		Map<String, String> properties = new HashMap<>();

		properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_DATAHUB);
		properties.put(CONNECTOR_PROPERTY_VERSION, "1");
		properties.put(CONNECTOR_PROJECT, "myproject");
		properties.put(CONNECTOR_TOPIC, "mytopic");
		properties.put(CONNECTOR_ACCESS_ID, "myid");
		properties.put(CONNECTOR_ACCESS_KEY, "mykey");
		properties.put(CONNECTOR_ENDPOINT, "myendpoint");

		properties.put("schema.0.name", "aaa");
		properties.put("schema.0.type", "INT");
		properties.put("schema.1.name", "bbb");
		properties.put("schema.1.type", "VARCHAR");
		properties.put("schema.2.name", "ccc");
		properties.put("schema.2.type", "DOUBLE");

		return properties;
	}
}
