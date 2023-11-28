package org.apache.flink.runtime.megaphone;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.io.Serializable;

public class ControlEvent extends RuntimeEvent implements Serializable {

	private long controlWatermark;
	private int sourceIndex;
	private int keyGroupIndex;
	private int targetIndex;
	private String targetIP;
	private int ID;

	public ControlEvent(){
		this.controlWatermark = Long.MIN_VALUE;
		this.sourceIndex = -1;
		this.keyGroupIndex = -1;
		this.targetIndex = -1;
		this.targetIP = "";
		this.ID = -1;
	}

	public ControlEvent(
		long controlWatermark,
		int sourceIndex,
		int keyGroupIndex,
		int targetIndex,
		String targetIP,
		int ID) {
		this.controlWatermark = controlWatermark;
		this.sourceIndex = sourceIndex;
		this.keyGroupIndex = keyGroupIndex;
		this.targetIndex = targetIndex;
		this.targetIP = targetIP;
		this.ID = ID;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(this.controlWatermark);
		out.writeInt(this.sourceIndex);
		out.writeInt(this.keyGroupIndex);
		out.writeInt(this.targetIndex);
		out.writeUTF(this.targetIP);
		out.writeInt(this.ID);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.controlWatermark = in.readLong();
		this.sourceIndex = in.readInt();
		this.keyGroupIndex = in.readInt();
		this.targetIndex = in.readInt();
		this.targetIP = in.readUTF();
		this.ID = in.readInt();
	}

	public long getControlWatermark() {
		return controlWatermark;
	}

	public void setControlWatermark(long controlWatermark) {
		this.controlWatermark = controlWatermark;
	}

	public int getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	public int getKeyGroupIndex() {
		return keyGroupIndex;
	}

	public void setKeyGroupIndex(int keyGroupIndex) {
		this.keyGroupIndex = keyGroupIndex;
	}

	public int getTargetIndex() {
		return targetIndex;
	}

	public void setTargetIndex(int targetIndex) {
		this.targetIndex = targetIndex;
	}

	public String getTargetIP() {
		return targetIP;
	}

	public void setTargetIP(String targetIP) {
		this.targetIP = targetIP;
	}

	public int getID() {
		return ID;
	}

	public void setID(int ID) {
		this.ID = ID;
	}

	@Override
	public String toString() {
		return "ControlEvent{" +
			"controlWatermark=" + controlWatermark +
			", sourceIndex=" + sourceIndex +
			", keyGroupIndex=" + keyGroupIndex +
			", targetIndex=" + targetIndex +
			", targetIP='" + targetIP + '\'' +
			", ID=" + ID +
			'}';
	}

}
