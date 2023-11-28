package org.apache.flink.runtime.megaphone;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

public class CleanEvent extends RuntimeEvent {

	public CleanEvent(){

	}

	@Override
	public void write(DataOutputView out) throws IOException {

	}

	@Override
	public void read(DataInputView in) throws IOException {

	}
}
