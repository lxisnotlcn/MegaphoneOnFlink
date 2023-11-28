package org.apache.flink.runtime.megaphone;

import java.util.ArrayList;
import java.util.List;

public class DownAckMessage extends AckMessage{
	private String ip;
	private int index;
	private List<Integer> keyGroupRange;
	public DownAckMessage(){
		this.ip = "";
		this.index = -1;
		this.keyGroupRange = new ArrayList<>();
	}

	public DownAckMessage(String ip, int index, List<Integer> keyGroupRange){
		this.ip = ip;
		this.index = index;
		this.keyGroupRange = keyGroupRange;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public List<Integer> getKeyGroupRange() {
		return keyGroupRange;
	}

	public void setKeyGroupRange(List<Integer> keyGroupRange) {
		this.keyGroupRange = keyGroupRange;
	}

	@Override
	public String toString() {
		return "AckMessage{" +
			"ip='" + ip + '\'' +
			", index=" + index +
			", keyGroupRange=" + keyGroupRange +
			'}';
	}
}
