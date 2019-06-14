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

package com.alibaba.flink.connectors.cloudhbase.sink;

import com.alibaba.flink.connectors.common.resolver.RecordResolver;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Interface for implementing CloudHBase's record resolver.
 *
 * @param <RECORD> the type of record to convert.
 */
public interface CloudHBaseRecordResolver<RECORD> extends RecordResolver<RECORD> {

	/**
	 * Returns the row key of the record in the form of string.
	 *
	 * @param record the record whose row key is returned.
	 */
	String getRowKey(RECORD record);

	/**
	 * Returns the mutation for the record.
	 *
	 * @param record the record whose mutation is returned.
	 */
	Mutation getMutation(RECORD record);

	/**
	 * Initialization method for the resolver.
	 */
	void open();

	/**
	 * Tear-down method for the resolver.
	 */
	void close();
}
