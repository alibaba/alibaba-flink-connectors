/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.alibaba.flink.connectors.common;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import com.alibaba.flink.connectors.common.source.AbstractParallelSourceBase;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit test for {@link AbstractParallelSourceBase.PreAssignedInputSplitAssigner}.
 */
public class PreAssignedInputSplitAssignerTest {
	private static final int NUM_TASKS = 3;
	private static final int NUM_SPLITS = (1 + NUM_TASKS) * NUM_TASKS / 2;

	@Test
	public void testInitializeAndAssign() {
		InputSplitAssigner assigner = getAssigner();
		assertEquals(0, assigner.getNextInputSplit("", 0).getSplitNumber());
		assertNull(assigner.getNextInputSplit("", 0));
		assertEquals(1, assigner.getNextInputSplit("", 1).getSplitNumber());
		assertEquals(2, assigner.getNextInputSplit("", 1).getSplitNumber());
		assertNull(assigner.getNextInputSplit("", 1));
		assertEquals(3, assigner.getNextInputSplit("", 2).getSplitNumber());
		assertEquals(4, assigner.getNextInputSplit("", 2).getSplitNumber());
		assertEquals(5, assigner.getNextInputSplit("", 2).getSplitNumber());
		assertNull(assigner.getNextInputSplit("", 2));
	}

	@Test (expected = IllegalArgumentException.class)
	public void testInvalidTaskIndexForGetNextInputSplit() {
		getAssigner().getNextInputSplit("", 3);
	}

	@Test (expected = IllegalArgumentException.class)
	public void testInvalidTaskIndexForReturnInputSplits() {
		getAssigner().returnInputSplit(Collections.emptyList(), 3);
	}

	@Test
	public void testReturnSplits() {
		InputSplitAssigner assigner = getAssigner();
		assertEquals(3, assigner.getNextInputSplit("", 2).getSplitNumber());
		assertEquals(4, assigner.getNextInputSplit("", 2).getSplitNumber());

		// Return split 3 back.
		assigner.returnInputSplit(Collections.singletonList(new MockInputSplit(3)), 2);

		assertEquals(3, assigner.getNextInputSplit("", 2).getSplitNumber());
		assertEquals(5, assigner.getNextInputSplit("", 2).getSplitNumber());
		assertNull(assigner.getNextInputSplit("", 2));
	}

	@Test (expected = IllegalStateException.class)
	public void testReturnSplitsToWrongTask() {
		InputSplitAssigner assigner = getAssigner();
		assertEquals(3, assigner.getNextInputSplit("", 2).getSplitNumber());
		assigner.returnInputSplit(Collections.singletonList(new MockInputSplit(3)), 1);
	}

	@Test (expected = IllegalStateException.class)
	public void testReturnUnassignedSplits() {
		InputSplitAssigner assigner = getAssigner();
		assigner.returnInputSplit(Collections.singletonList(new MockInputSplit(3)), 1);
	}

	private InputSplitAssigner getAssigner() {
		InputSplit[] inputSplits = new InputSplit[NUM_SPLITS];
		int[] taskInputSplitSize = new int[NUM_TASKS];
		int[] taskInputSplitStartIndex = new int[NUM_TASKS];
		for (int i = 0; i < NUM_SPLITS; i++) {
			inputSplits[i] = new MockInputSplit(i);
		}
		int currentSplitIndex = 0;
		for (int i = 0; i < NUM_TASKS; i++) {
			taskInputSplitSize[i] = i + 1;
			taskInputSplitStartIndex[i] = currentSplitIndex;
			currentSplitIndex += taskInputSplitSize[i];
		}
		return new AbstractParallelSourceBase.PreAssignedInputSplitAssigner(
				inputSplits, taskInputSplitSize, taskInputSplitStartIndex);
	}

	private static class MockInputSplit implements InputSplit {
		private final int splitNumber;

		MockInputSplit(int splitNumber) {
			this.splitNumber = splitNumber;
		}

		@Override
		public int getSplitNumber() {
			return splitNumber;
		}
	}
}
