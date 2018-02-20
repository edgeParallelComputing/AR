package storm.spout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.logging.Logger;
import java.util.Map;
import java.util.logging.FileHandler;
import java.io.*;
import java.net.*;

import topologyResourceConsumption.*;
import reconstructionAPI.*;
import storm.stormLogger.*;

public class FileSpout extends BaseRichSpout {

	private String spoutName;
	private String outFieldsName[];

	private SpoutOutputCollector collector;

	private String leftImgAddr;
	private String rightImgAddr;
	private int imgId = 0;
	private byte leftImg[];
	private byte rightImg[];

	private byte[] stereo;
	private	boolean isCheckResourceConsumption;
	private TopologyResourceConsumption rm;
	private Logger log;
	private String logFile;

	private final int rows, cols;

	private int sendInterval;

	private boolean init = true;

	private String serverIp;
	private int serverPort;

	public FileSpout(String spoutName, String outFieldsName[], String leftImgAddr, String rightImgAddr, int imgRows, int imgCols, int sendInterval,
	                 boolean isCheckResourceConsumption, String logFile, String serverIp, int serverPort) {

		this.spoutName = spoutName;
		this.outFieldsName = outFieldsName;
		this.leftImgAddr = leftImgAddr;
		this.rightImgAddr = rightImgAddr;

		this.rows = imgRows;
		this.cols = imgCols;

		this.sendInterval = sendInterval;

		this.isCheckResourceConsumption = isCheckResourceConsumption;
		this.logFile = logFile;

		this.serverIp = serverIp;
		this.serverPort = serverPort;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.leftImg = new byte[rows * cols * 3];
		this.rightImg = new byte[rows * cols * 3];

		ReconstructionAPI.readImage(leftImgAddr, leftImg);
		ReconstructionAPI.readImage(rightImgAddr, rightImg);

		stereo = StereoPartition.mergeStereo(leftImg, rightImg);

		if(isCheckResourceConsumption){
			log = new StormLogger(logFile, "reconstruction").getLogger();
			rm = new TopologyResourceConsumption(spoutName, log);
		}
	}

	@Override
	public void nextTuple() {

		while (true) {

			if (isCheckResourceConsumption && imgId % 1 == 0) {
				rm.updateConsumingResource(imgId);
			}

			if(imgId % 1 == 0){
				//this.collector.emit("overallLatencyTuples", new Values(imgId, trc.getElapseTime()));
				try{
					DatagramSocket clientSocket = new DatagramSocket();
		      		InetAddress IPAddress = InetAddress.getByName(serverIp);
		      		//byte[] sendData = new byte[100];
		      		//sendData = String.valueOf(imgId).getBytes();
		      		String log_s = spoutName + " " + imgId + " " + rm.elapseTimeDiff(); 
		      		byte[] sendData = log_s.getBytes();
		      		DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, serverPort);
		      		clientSocket.send(sendPacket);
		      		sendPacket = null;
		      		clientSocket.close();
		      		clientSocket = null;
				}catch(Exception e){
					
				}
				
			}

			/* emit byte array of stereo images */
			
			this.collector.emit(outFieldsName[0], new Values(imgId, -1, stereo));

			if (isCheckResourceConsumption && imgId % 1 == 0) {
			    rm.setInOutputSize(0, stereo.length);
			    rm.reportConsumingResource(imgId);
			}
			
			imgId++;
			/* slepp in (millis) for a while and send the next image */
			try {
				Thread.sleep(sendInterval);
			} catch (InterruptedException ex1) {
 
			}
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    //declarer.declareStream("overallLatencyTuples", new Fields(outFieldsName[1], "overallLatency"));
	    declarer.declareStream(outFieldsName[0], new Fields(outFieldsName[1], outFieldsName[2], outFieldsName[3]));
	}

        @Override
	public void ack(Object msgId){
	    System.out.println("done" + msgId);
	}

        @Override
	public void fail(Object msgId){
	    System.out.println("fail" + msgId);
	}

	@Override
	public void close() {
	}

}
