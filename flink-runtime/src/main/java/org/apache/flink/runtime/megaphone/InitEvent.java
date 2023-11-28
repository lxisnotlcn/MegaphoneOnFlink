package org.apache.flink.runtime.megaphone;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.HashMap;

public class InitEvent extends RuntimeEvent {

	private int index;
	private String ip;
	private int port;

	public InitEvent(){
		this.index = -1;
		this.ip = "";
		this.port = -1;
	}

	public InitEvent(int index, String ip, int port){
		this.index = index;
		this.ip = ip;
		this.port = port;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(index);
		out.writeUTF(ip);
		out.writeInt(port);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.index = in.readInt();
		this.ip = in.readUTF();
		this.port = in.readInt();
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return "InitEvent{" +
			"index=" + index +
			", ip='" + ip + '\'' +
			", port=" + port +
			'}';
	}
}
