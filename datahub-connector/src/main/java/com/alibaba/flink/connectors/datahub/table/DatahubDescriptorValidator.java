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

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * Descriptor validator for Datahub.
 */
public class DatahubDescriptorValidator extends ConnectorDescriptorValidator {
	public static final String CONNECTOR_TYPE_VALUE_DATAHUB = "datahub";

	public static final String CONNECTOR_PROJECT = "connector.project";
	public static final String CONNECTOR_TOPIC = "connector.topic";
	public static final String CONNECTOR_ACCESS_ID = "connector.access_id";
	public static final String CONNECTOR_ACCESS_KEY = "connector.access_key";
	public static final String CONNECTOR_ENDPOINT = "connector.endpoint";

	public static final String CONNECTOR_BUFFER_SIZE = "connector.buffer_size";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch_size";
	public static final String CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS = "connector.batch_write_timeout_in_mills";
	public static final String CONNECTOR_RETRY_TIMEOUT_IN_MILLS = "connector.retry_timeout_in_mills";
	public static final String CONNECTOR_MAX_RETRY_TIMES = "connector.max_retry_times";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, getConnectorTypeValue(), false);
		properties.validateString(CONNECTOR_PROJECT, false, 1);
		properties.validateString(CONNECTOR_TOPIC, false, 1);
		properties.validateString(CONNECTOR_ACCESS_ID, false, 1);
		properties.validateString(CONNECTOR_ACCESS_KEY, false, 1);
		properties.validateString(CONNECTOR_ENDPOINT, false, 1);

		properties.validateInt(CONNECTOR_BUFFER_SIZE, true, 1);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateLong(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS, true, 1);
		properties.validateInt(CONNECTOR_RETRY_TIMEOUT_IN_MILLS, true, 1);
		properties.validateInt(CONNECTOR_MAX_RETRY_TIMES, true, 1);
	}

	public String getConnectorTypeValue() {
		return CONNECTOR_TYPE_VALUE_DATAHUB;
	}
}
