package org.apache.flink.runtime.megaphone;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.messages.Acknowledge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MegaphoneController {
	private final Object lock = new Object();
	private JobStatusListener jobStatusListener;
	private final JobID jobID;
	private ExecutionVertex[] taskToTrigger;
	private ExecutionVertex[] upStreamExecutionVertex;
	private ExecutionVertex[] downStreamExecutionVertex;
	private HashMap<Integer, Long> upStreamWatermark;
	private HashMap<Integer, String> downStreamIP;
	private HashMap<Integer, List<Integer>> downStreamKeyGroups;
	private final ScheduledExecutor timer;
	private ScheduledFuture<?> currentTrigger;
	private ResultCollector resultCollector;
	private boolean haveInit = false;
	private int controlID = 1;
	private static final String CONF_FILE = "megaphone.json";
	private static final String ENV_CONF = "CONF_DIR";
	private Config config;
	private AtomicInteger controlCount;


	public MegaphoneController(JobID jobID,
							   ExecutionVertex[] taskToTrigger,
							   ExecutionVertex[] upStreamExecutionVertex,
							   ExecutionVertex[] downStreamExecutionVertex,
							   ScheduledExecutor timer){
		this.jobID = jobID;
		this.taskToTrigger = taskToTrigger;
		this.upStreamExecutionVertex = upStreamExecutionVertex;
		this.downStreamExecutionVertex = downStreamExecutionVertex;
		this.timer = timer;
		this.resultCollector = new ResultCollector();
	}

	public JobStatusListener createActivatorDeactivator() {
		synchronized (lock){
			if(jobStatusListener == null){
				jobStatusListener = new MegaphoneControllerDeActivator(this);
			}
			return jobStatusListener;
		}
	}

	public void startScheduler() {
		try {
			synchronized (lock){
				initUpStreamCollection();
				//Thread.sleep(1000L);
				readerMethod();
				if(!config.isEnable()){
					return;
				}
				currentTrigger = timer.scheduleAtFixedRate(
					new InitInformation(),
					0L,
					4000L,
					TimeUnit.MILLISECONDS
				);
			}
		} catch (Exception e) {
			e.printStackTrace();
			//System.out.println("init error");
		}
	}

	private void initUpStreamCollection(){
		upStreamWatermark = new HashMap<>();
		downStreamIP = new HashMap<>();
		downStreamKeyGroups = new HashMap<>();
		//初始化上游水位线
		int count = upStreamExecutionVertex.length;
		for(int i=0;i<count;++i){
			upStreamWatermark.put(i, Long.MIN_VALUE);
		}


		//初始化下游IP地址
//		count = downStreamExecutionVertex.length;
//		for(int i=0;i<count;++i){
//			downStreamIP.put(i, "");
//		}
	}

	private void readerMethod() throws IOException {
		//System.out.println(System.getenv(ENV_CONF));
		File file = new File(System.getenv(ENV_CONF), CONF_FILE);
		StringBuffer sb = new StringBuffer();
		String tmp;
		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			while((tmp = reader.readLine()) != null){
				sb.append(tmp);
			}
		}catch (IOException e){
			e.printStackTrace();
		}
		String jsonStr = sb.toString();
		config = JSON.parseObject(jsonStr, Config.class);
	}

	public void stopScheduler() {
		synchronized (lock){
			if(currentTrigger != null){
				currentTrigger.cancel(false);
				currentTrigger = null;
			}
		}
	}

	private Execution[] getTriggerExecutions() {
		Execution[] executions = new Execution[taskToTrigger.length];
		//System.out.println("taskToTrigger.length:"+taskToTrigger.length);
		for(int i=0;i<taskToTrigger.length;++i){
			Execution ee = taskToTrigger[i].getCurrentExecutionAttempt();
			if(ee == null){
				throw new RuntimeException("Execution null");
			} else if (ee.getState() == ExecutionState.RUNNING) {
				executions[i] = ee;
			}else{
				throw new RuntimeException("Execution not running");
			}
		}
		return executions;
	}

//	private ScheduledFuture<?> scheduleTriggerWithDelay() {
////		return timer.scheduleAtFixedRate(
////			new MegaphoneTrigger(),
////			500L,
////			10000L,
////			TimeUnit.MILLISECONDS
////		);
//	}

	private class InitInformation implements Runnable{

		@Override
		public void run() {
			boolean flag = false;
			try {
				//System.out.println("Init information");
				Execution[] executions = getTriggerExecutions();
				for(Execution execution:executions){
					execution.triggerEvent(EventType.INIT_EVENT);
				}
				flag = true;
			} catch (Exception e) {
				//System.out.println("reInit");
			}
			if(flag) {
				boolean isCancel = currentTrigger.cancel(false);
				//System.out.println("isCancel:" + isCancel);
			}
		}
	}

	public void acknowledgeToController(JobID jobID, ExecutionAttemptID executionAttemptID){
		controlCount.addAndGet(1);
		if(controlCount.get() != config.getKeyGroups().length){
			new Thread(new ControlThread(
				0,
				config.getGap(),
				config.getSourceIndex()[controlCount.get()],
				config.getKeyGroups()[controlCount.get()],
				config.getTargetIndex()[controlCount.get()]
			)).start();
		}
	}

	public void acknowledgeToController(JobID jobID, ExecutionAttemptID executionAttemptID, AckMessage ack){
		if(ack instanceof UpAckMessage){
			upStreamWatermark.put(((UpAckMessage) ack).getIndex(), ((UpAckMessage) ack).getWatermark());
		}else if(ack instanceof DownAckMessage){
			downStreamIP.put(((DownAckMessage) ack).getIndex(), ((DownAckMessage) ack).getIp());
			downStreamKeyGroups.put(((DownAckMessage) ack).getIndex(), ((DownAckMessage) ack).getKeyGroupRange());
			if(downStreamIP.size() == downStreamExecutionVertex.length){
				haveInit = true;
				controlCount = new AtomicInteger(0);
				new Thread(new ControlThread(
					config.getDelay(),
					config.getGap(),
					config.getSourceIndex()[controlCount.get()],
					config.getKeyGroups()[controlCount.get()],
					config.getTargetIndex()[controlCount.get()]
					)).start();
			}
		}
	}

	private static class ResultCollector{
		private Set<ExecutionAttemptID> executionsToWaitFor;
		private int numExecutionsToWaitFor = -1;
		private CompletableFuture<Acknowledge> future;

		CompletableFuture<Acknowledge> startNewStage(Set<ExecutionAttemptID> executionsToWaitFor) {
			this.executionsToWaitFor = executionsToWaitFor;
			this.numExecutionsToWaitFor = executionsToWaitFor.size();
			this.future = new CompletableFuture<>();
			return future;
		}

		void acknowledge(ExecutionAttemptID executionAttemptID) {
			synchronized (this) {
//				if (executionsToWaitFor.contains(executionAttemptID)) {
//					executionsToWaitFor.remove(executionAttemptID);
//					numExecutionsToWaitFor--;
////						System.out.println("numExecutionsToWaitFor" + numExecutionsToWaitFor);
//					if (numExecutionsToWaitFor <= 0) {
//						System.out.println("stage complete");
//						future.complete(Acknowledge.get());
//					}
//				}else {
//					throw new UnsupportedOperationException("executionsToWaitFor not contain");
//				}
				//System.out.println("ack from "+executionAttemptID);
			}
		}

		void acknowledge(ExecutionAttemptID executionAttemptID, AckMessage ack) {
			synchronized (this) {
//				if (executionsToWaitFor.contains(executionAttemptID)) {
//					executionsToWaitFor.remove(executionAttemptID);
//					numExecutionsToWaitFor--;
////						System.out.println("numExecutionsToWaitFor" + numExecutionsToWaitFor);
//					if (numExecutionsToWaitFor <= 0) {
//						System.out.println("stage complete");
//						future.complete(Acknowledge.get());
//					}
//				}else {
//					throw new UnsupportedOperationException("executionsToWaitFor not contain");
//				}
//				if(ack instanceof DownAckMessage) {
//					System.out.println("ack from " + executionAttemptID);
//					System.out.println("ack ip:" + ((DownAckMessage) ack).getIp());
//					System.out.println("ack keygroup:" + Arrays.toString(((DownAckMessage) ack)
//						.getKeyGroupRange()
//						.toArray()));
//				}else if(ack instanceof UpAckMessage){
//					System.out.println(ack);
//				}
				//System.out.println(ack);
			}
		}
	}

	private class ControlThread implements Runnable{

		private int delay;
		private int gap;
		private int sourceIndex;
		private int keyGroupIndex;
		private int targetIndex;
		private int downStreamIPIndex;

		public ControlThread(
			int delay,
			int gap,
			int sourceIndex,
			int keyGroupIndex,
			int targetIndex) {
			this.delay = delay;
			this.gap = gap;
			this.sourceIndex = sourceIndex;
			this.keyGroupIndex = keyGroupIndex;
			this.targetIndex = targetIndex;
		}

		@Override
		public void run() {
			try {
				Thread.sleep(delay);
				//System.out.println("send config");
				long maxWatermark = Long.MIN_VALUE;
				for(Long watermark: upStreamWatermark.values()){
					maxWatermark = Math.max(maxWatermark, watermark);
				}
				maxWatermark += gap;
				ControlEvent ce = new ControlEvent(maxWatermark, sourceIndex, keyGroupIndex, targetIndex, downStreamIP.get(targetIndex), controlID++);
				//System.out.println(ce);
				Execution[] executions = getTriggerExecutions();
				for(Execution execution:executions){
					execution.triggerEvent(ce);
				}
				//System.out.println("finish");
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
}
