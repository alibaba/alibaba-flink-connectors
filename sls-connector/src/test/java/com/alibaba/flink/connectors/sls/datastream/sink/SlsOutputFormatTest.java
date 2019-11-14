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

package com.alibaba.flink.connectors.sls.datastream.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.common.MetricUtils;
import com.alibaba.flink.sls.shaded.com.google.common.util.concurrent.SettableFuture;
import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * Test for sls output format.
 */
public class SlsOutputFormatTest {

	@Test
	public void testCommitFailed() {
		SlsRecordResolver serializationSchema = Mockito.mock(SlsRecordResolver.class);
		SlsOutputFormat outputFormat = new SlsOutputFormat(
				new Configuration(), "", "", "", "", "", serializationSchema);

		SlsOutputFormat.SendFutureCallback callback = outputFormat.new SendFutureCallback();
		callback.onFailure(new Exception("1"));
		AtomicLong numCommitted = (AtomicLong) Whitebox.getInternalState(
				outputFormat, "numCommitted");
		assertEquals(1, numCommitted.get());
		RuntimeException callbackException = (RuntimeException) Whitebox.getInternalState(
				outputFormat, "callBackException");
		assertEquals("1", callbackException.getCause().getMessage());
	}

	@Test
	public void testCommit() throws ProducerException, InterruptedException, IOException {

		SlsRecordResolver<Row> serializationSchema = Mockito.mock(SlsRecordResolver.class);
		SlsOutputFormat<Row> outputFormat = new SlsOutputFormat(
				new Configuration(), "", "", "", "test_project", "test_store", serializationSchema);
		LogProducer producer = Mockito.mock(LogProducer.class);
		LogProducerProvider producerProvider = Mockito.mock(LogProducerProvider.class);
		Mockito.when(producerProvider.getClient()).thenReturn(producer);

		SettableFuture future = SettableFuture.create();
		// Use any() instead of anyString() because in Mockito 2.x, anyString() does not match null any more,
		// which may cause the test to fail.
		Mockito.when(
				producer.send(
						Mockito.eq("test_project"),
						Mockito.eq("test_store"),
						Mockito.any(),
						Mockito.any(),
						Mockito.any(),
						Mockito.anyList())).thenReturn(future);

		ExecutorService executor = Executors.newSingleThreadExecutor();
		Whitebox.setInternalState(outputFormat, "logProducerProvider", producerProvider);
		Whitebox.setInternalState(
				outputFormat, "sendFutureCallback", outputFormat.new SendFutureCallback());
		Whitebox.setInternalState(outputFormat, "executor", executor);
		Whitebox.setInternalState(outputFormat, "latencyGauge", Mockito.mock(MetricUtils.LatencyGauge.class));
		Whitebox.setInternalState(outputFormat, "outTps", Mockito.mock(Meter.class));

		Row record = new Row(3);
		record.setField(0, 100);
		record.setField(1, 1000);
		record.setField(2, "test");
		outputFormat.writeRecord(record);
		AtomicLong numSent = (AtomicLong) Whitebox.getInternalState(outputFormat, "numSent");
		AtomicLong numCommitted = (AtomicLong) Whitebox.getInternalState(outputFormat, "numCommitted");
		assertEquals(1, numSent.get());
		assertEquals(0, numCommitted.get());

		// trigger call back.
		future.set(new Result(true, null, 0));
		// wait call back finished.
		executor.awaitTermination(1, TimeUnit.SECONDS);
		assertEquals(1, numSent.get());
		assertEquals(1, numCommitted.get());
	}
}
