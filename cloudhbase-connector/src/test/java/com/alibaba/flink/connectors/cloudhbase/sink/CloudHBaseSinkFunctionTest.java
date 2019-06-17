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

import org.apache.hadoop.hbase.client.Mutation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link CloudHBaseSinkFunction}.
 */
public class CloudHBaseSinkFunctionTest {

	@Test
	public void testConstruct() {
		final String zkQuorum = "a,b,c";
		final String zkZNodeParent = "/hbase";
		final String tableName = "testTableName";
		final int maxRetryNumber = 1;
		final int bufferSize = 2;
		final int batchSize = 3;
		final long flushInterval = 4;
		final CloudHBaseRecordResolver<Mutation> recordConverter = mock(CloudHBaseRecordResolver.class);

		CloudHBaseSinkFunction<Mutation> sinkFunction1 = new CloudHBaseSinkFunction<>(zkQuorum, tableName, recordConverter);

		assertEquals(zkQuorum, sinkFunction1.getZkQuorum());
		assertNull(sinkFunction1.getZkZNodeParent());
		assertEquals(tableName, sinkFunction1.getTableName());
		assertEquals(CloudHBaseSinkFunction.DEFAULT_MAX_RETRY_NUMBER, sinkFunction1.getMaxRetryNumber());
		assertEquals(CloudHBaseSinkFunction.DEFAULT_BATCH_SIZE, sinkFunction1.getBatchSize());
		assertEquals(CloudHBaseSinkFunction.DEFAULT_BUFFER_SIZE, sinkFunction1.getBufferSize());
		assertEquals(CloudHBaseSinkFunction.DEFAULT_FLUSH_INTERVAL, sinkFunction1.getFlushInterval());
		assertSame(recordConverter, sinkFunction1.getCloudHBaserecordResolver());

		CloudHBaseSinkFunction<Mutation> sinkFunction2 = new CloudHBaseSinkFunction<>(
				zkQuorum, zkZNodeParent, tableName, bufferSize, batchSize, flushInterval, maxRetryNumber, recordConverter);

		assertEquals(zkQuorum, sinkFunction2.getZkQuorum());
		assertEquals(zkZNodeParent, sinkFunction2.getZkZNodeParent());
		assertEquals(tableName, sinkFunction2.getTableName());
		assertEquals(maxRetryNumber, sinkFunction2.getMaxRetryNumber());
		assertEquals(batchSize, sinkFunction2.getBatchSize());
		assertEquals(bufferSize, sinkFunction2.getBufferSize());
		assertEquals(flushInterval, sinkFunction2.getFlushInterval());
		assertSame(recordConverter, sinkFunction2.getCloudHBaserecordResolver());
	}
}
