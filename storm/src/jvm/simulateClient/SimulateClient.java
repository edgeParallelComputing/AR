package simulateClient;

import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Date;

public class SimulateClient{

    ///	private static final Logger LOG = LoggerFactory.getLogger(SimulateClient.class);
	private int clientPort;
	private ConcurrentHashMap<Integer, ArrayList<Long>> taskLatencyCount = new ConcurrentHashMap<>();
	private DatagramSocket serverSocket;
	private BufferedWriter writer;
	private String logFileName;
    private AtomicInteger count = new AtomicInteger();
    private long startTime = 0;

	public SimulateClient(int clientPort){
		logFileName = "log.csv";
		this.clientPort = clientPort;
		try{
			writer = new BufferedWriter(new FileWriter(logFileName, true));
			this.serverSocket = new DatagramSocket(clientPort);
		}catch(Exception e){

		}
	}

	public void run(){
		while(true){
	         SimulateClientThread simulateClientThread = new SimulateClientThread(serverSocket);
	         simulateClientThread.run();
		}
	}

	private class SimulateClientThread extends Thread{
		private DatagramSocket socket = null;
		private byte receiveData[] = new byte[100];

		public SimulateClientThread(DatagramSocket socket){
			this.socket = socket;
		}

		@Override
		public void run(){
			 DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			 try{
			    
			    socket.receive(receivePacket);
			 	String data = new String(receivePacket.getData()).trim();
				System.out.println(data);
			 	//Integer index = Integer.valueOf(data);
			 	String[] splited = data.split("\\s+");
			 	writer.append(splited[0]+","+splited[1]+","+splited[2]);
			 	writer.newLine();

			 	Integer index = Integer.valueOf(splited[1]);
			 	if(splited[0].equals("VideoSpout")){
			 		ArrayList<Long> latencyList = new ArrayList<>();
			 		latencyList.add(new Date().getTime());
					taskLatencyCount.put(index, latencyList);
					if(index == 0){
					    startTime = System.nanoTime();
					    count.getAndSet(0);
					}
			 	}else if(splited[0].equals("reprojectionBolt")){

			 		Long initialTime = taskLatencyCount.get(index).get(0);
			 		Long elapseTime = new Date().getTime() - initialTime;
					count.incrementAndGet();
					long totalElapseTime = (System.nanoTime() - startTime)/1000000;
					int averageTaskThroughput = count.get() / (int)totalElapseTime;
			 		System.out.println("Overall Latency of Task " + index + " is " + elapseTime);
					writer.append("throughtput," + String.valueOf(averageTaskThroughput)); 
			 		writer.append("Overall,"+splited[1]+","+elapseTime);
			 		writer.newLine();
			 		taskLatencyCount.remove(index);
					writer.flush();
			 	}

				
			 	/*

			 	if(taskLatencyCount.get(index) == null){
			 		ArrayList<Long> latencyList = new ArrayList<>();
			 		latencyList.add(new Date().getTime());
					taskLatencyCount.put(index, latencyList);
			 	}else{
			 		//taskLatencyCount.get(index).add(new Date().getTime());
			 		Long initialTime = taskLatencyCount.get(index).get(0);
			 		System.out.println("Overall Latency of Task " + index + " is " +  (new Date().getTime() - initialTime));
			 		writer.append('test,');
			 		//write.newLine();
			 	}
			 	*/

			 }catch(Exception e){

			 }

		}
	}

	public static void main(String args[]){
		SimulateClient simulateClient = new SimulateClient(6666);
		simulateClient.run();
	}
}
