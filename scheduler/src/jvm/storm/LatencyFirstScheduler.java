package storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.Hashtable;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.EvenScheduler;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.scheduler.SchedulerAssignment;

import java.util.logging.Logger;
import java.util.logging.FileHandler;

public class LatencyFirstScheduler implements IScheduler {

    private static final String SCHEDULER_LOGFILE = "/root/scheduler.log";
    private static final String TOPOLOGY_NAME = "AR";
    private static final String TOPOLOGY_CONF = "/root/ar/utilization/node21-1/allLog_";

    private static final boolean debug = true;

    public static class StormLogger {
        private static Logger Log;

        private void initLogger() {
            try {
                FileHandler handler = new FileHandler(SCHEDULER_LOGFILE, true);
                Log = Logger.getLogger("storm");
                Log.addHandler(handler);
            } catch (java.io.IOException ex1) {
                System.out.println("[Error] Failt to open" + SCHEDULER_LOGFILE);
            }

        }

        private StormLogger() {
            initLogger();
        }

        private static StormLogger stormLogger = new StormLogger();

        public static Logger getLogger() {
            return stormLogger.Log;
        }
    }

    private static final Logger log = StormLogger.getLogger();

    /* resource monitor */
    private ResourceMonitor resourceMonitor = ResourceMonitor.getResourceMonitor();

    /* topology information */
    private List<TopologyManager> topologyManagerList = new ArrayList<>();
    private Map<String, List<ExecutorDetails>> executorToNodeMap = new Hashtable<>();
    private Map<String, List<String>> nodeContainsTaskMap = new Hashtable<>();



    private Set<String> sortTask(TopologyManager topologyManager) {
        HashMap<String, Double> map = new HashMap<>();
        LatencyComparator latencyComparator = new LatencyComparator(map);
        TreeMap<String, Double> sort_map = new TreeMap<>(latencyComparator);

        for (String task : topologyManager.getTaskList()) {
            double computeLatency = topologyManager.getTaskLatency(task, 1);
            map.put(task, computeLatency);
        }

        sort_map.putAll(map);
        return sort_map.keySet();
    }


    private class LatencyComparator implements Comparator<String> {
        Map<String, Double> base;

        public LatencyComparator(Map<String, Double> base) {
            this.base = base;
        }

        public int compare(String a, String b) {
            if (base.get(a) > base.get(b)) {
                return -1;
            } else {
                return 1;
            }
        }
    }


    private double computeOverallLatencyForTask(String task, TopologyManager topologyManager, String node) {

       //check memory is available.
	log.info("compute on node" + node + task);
        double memoryFree = resourceMonitor.nodeTable.get(node).getMemoryFree();
        double memoryDemand = topologyManager.getTaskMemory(task);
        if(memoryFree < memoryDemand){
            return Integer.MAX_VALUE;
        }

        //estimate computing latency in ms
        double cpuUtilization = resourceMonitor.nodeTable.get(node).getCpuUtilizationFree();
        double computingLatency = topologyManager.getTaskLatency(task, cpuUtilization);

        //estimate network transmission latency in ms
        double uploadBandwidth = resourceMonitor.nodeTable.get(node).getUploadBandwidth();
        double transmissionLatency = topologyManager.getTaskOutput(task) / uploadBandwidth * 1000;

        double sum = computingLatency + transmissionLatency;

        if (debug) {
            log.info("node " + node + " utilization of " + cpuUtilization + " cpu latency = " + computingLatency + "bandwidth " + uploadBandwidth + " uploadLatency = " + transmissionLatency + " sum = " + sum);
        }

        return sum;
    }


    private String findOptimalNodeForComponent(String task, TopologyManager topologyManager, List<String> supervisorHostList) {


        // we need to get available resource of all nodes 
        double lowestLatency = Integer.MAX_VALUE;
        String optimalNode = null;

        for (String node : supervisorHostList) {
            double latency = computeOverallLatencyForTask(task, topologyManager, node);
            if (latency < lowestLatency) {
                lowestLatency = latency;
                optimalNode = node;
            }
        }

        return optimalNode;
    }


    private void updateNodeResource(String node, TopologyManager topologyManager, String task, Map<String, List<ExecutorDetails>> componentToExecutors) {

        //update memory
        double memoryFree = resourceMonitor.nodeTable.get(node).getMemoryFree();
        double memoryDemand = topologyManager.getTaskMemory(task);
        resourceMonitor.nodeTable.get(node).setMemoryFree(memoryFree - memoryDemand);

        //update cpu utilization
        double cpuUtilizationFree = resourceMonitor.nodeTable.get(node).getCpuUtilizationFree();
        double cpuUtilizationDemand = topologyManager.getTaskCpuUtilization(task) / 32; //todo: get the core number from resource monitor...
        resourceMonitor.nodeTable.get(node).setCpuUtilizationFree(cpuUtilizationFree + cpuUtilizationDemand);

        //update bandwidth
        double uploadBandwidthFree = resourceMonitor.nodeTable.get(node).getUploadBandwidth();
	int taskCount = componentToExecutors.get(task).size();
        double uploadBandwidthDemand = topologyManager.getTaskUpBandwidth(task) / taskCount;
        resourceMonitor.nodeTable.get(node).setUploadBandwidth(uploadBandwidthFree - uploadBandwidthDemand);
	log.info("node " + node + "free uploadBanwidth " + uploadBandwidthFree + "uploadBandwidthDemand " + uploadBandwidthDemand);
    }


    private void scheduleComponent(String task, TopologyManager topologyManager, Map<String, List<ExecutorDetails>> componentToExecutors, List<String> supervisorsList) {

        List<ExecutorDetails> executors = componentToExecutors.get(task);
        if (executors == null) {
            return;
        }

        for (ExecutorDetails ed : executors) {

            String optimalNode = findOptimalNodeForComponent(task, topologyManager, supervisorsList);

            if (!executorToNodeMap.containsKey(optimalNode)) {
                List<ExecutorDetails> executorDetailsList = new ArrayList<>();
                executorToNodeMap.put(optimalNode, executorDetailsList);
            }

            //put executorDetails to node list
            executorToNodeMap.get(optimalNode).add(ed);

            //update the resoure cost on the optimal node
            updateNodeResource(optimalNode, topologyManager, task, componentToExecutors);

            //record the result
            if (nodeContainsTaskMap.get(optimalNode) == null) {
                List<String> taskList = new ArrayList<>();
                taskList.add(task);
                nodeContainsTaskMap.put(optimalNode, taskList);
            } else {
                nodeContainsTaskMap.get(optimalNode).add(task);
            }

            if (debug) {
                log.info("to assign component " + task + " optimal node " + optimalNode);
            }
        }

    }

    /*
      @In scheduleComponent function , it just assign a task to a specific node. 
      Then in each node, we assign all tasks to available workslots in round robin way
    */
    private void assignComponentToSlot(Cluster cluster, TopologyDetails topology, Map<String, List<WorkerSlot>> supervisorToAvailableslots) {

        for (String node : executorToNodeMap.keySet()) {
            List<ExecutorDetails> executorDetailsList = executorToNodeMap.get(node);
            //log.info("assign task to slot");
            //log.info("node " + node + "executor" + executorDetailsList);
            int availableSlotNum = supervisorToAvailableslots.get(node).size();

            for (int slot = 0; slot < availableSlotNum; slot++) {
                List<ExecutorDetails> executorListToSlot = new ArrayList<>();
                for (int executor = 0 + slot; executor < executorDetailsList.size(); executor += availableSlotNum) {
                    executorListToSlot.add(executorDetailsList.get(executor));
                }

                WorkerSlot workerSlot = supervisorToAvailableslots.get(node).get(slot);
                if (workerSlot != null) {
                    cluster.assign(workerSlot, topology.getId(), executorListToSlot);
                    if (debug) {
                        log.info("Assign executor " + executorListToSlot + " to slot: [ " + workerSlot.getNodeId() + ", " + workerSlot.getPort() + "]");
                    }
                } else {
                    log.info("cannot get the slot");
                }
            }
        }
    }

    @Override
    public void prepare(Map config) {
        try {
            log.info("initiate resource monitor of all nodes");
            resourceMonitor.collectResource();
            log.info("resource monitor task is done!");
	    log.info(resourceMonitor.toString());
	    for (String node : resourceMonitor.nodeTable.keySet()) {
		log.info("resource monitor node:" + node);
	    }
        } catch (Exception e) {
            log.info("FAIL to get resource from node!");
        }
    }


    @Override
    public void schedule(Topologies topologies, Cluster cluster) {

        //-------------------------------------LatencyFirst Scheduler starts to schedule!------------------------------------;

        Collection<WorkerSlot> usedslots = cluster.getUsedSlots();
        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
        List<String> supervisorHostList = new ArrayList<String>();
        Map<String, Integer> supervisorAvailableSlotNum = new HashMap<String, Integer>();  // store supervisor available slots
        Map<String, List<WorkerSlot>> supervisorAvailableSlots = new HashMap<String, List<WorkerSlot>>();  //slots<WorkerSlot>


        for (SupervisorDetails supervisor : supervisors) {
            String host = supervisor.getHost();
            supervisorHostList.add(host);
            List<WorkerSlot> availableslots = cluster.getAvailableSlots(supervisor);
            supervisorAvailableSlots.put(host, availableslots);
            int slotSize = availableslots.size();
            supervisorAvailableSlotNum.put(host, slotSize);
        }

        if (debug) {
            for (String host : supervisorHostList) {
                //log.info("------------------supervisor named " + host + " ------------------");
                //log.info(host + " has  < " + supervisorAvailableSlotNum.get(host) + " > availableslots and the slots list :" + supervisorAvailableSlots.get(host));
                //log.info("-------------------------------------end------------------------------------");
            }
        }

        //start to schedule the topology
        TopologyDetails topology = topologies.getByName(TOPOLOGY_NAME);
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);
            TopologyManager topologyManager = new TopologyManager(TOPOLOGY_NAME, TOPOLOGY_CONF);
            this.topologyManagerList.add(topologyManager);

            try {
                topologyManager.init();
            } catch (Exception e) {
                System.err.println(e);
            }

            if (!needsScheduling) {
                log.info(" topology {} DOES NOT NEED schedule!" +  TOPOLOGY_NAME);
            } else {
                log.info("starts to schedule {} topology !" + TOPOLOGY_NAME);
            }


            //get all tasks to be scheduled
            Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
             Set<String>componentList = componentToExecutors.keySet();
            log.info("output scheduling list " + componentToExecutors);

            //scheduling result.
            SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());

            if (debug) {
                if (currentAssignment != null) {
                    log.info("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    log.info("current assignments: {}");
                }
            }


            /* sort the task list according to the latency cost. We give the priority to the task with high latency */

            Set<String> taskAllocationInOrderList = sortTask(topologyManager);

            if (debug) {
                log.info("Schedule task in the order of :" + taskAllocationInOrderList);
            }

            executorToNodeMap.clear();

            // schedule task to a workSlot...
            for (String task : taskAllocationInOrderList) {
		log.info("we are going to schedule " + task);
		scheduleComponent(task, topologyManager, componentToExecutors, supervisorHostList);
            }

            //delete
            log.info("finish schedule and start to assign");
            log.info("node mapping" + nodeContainsTaskMap);
            assignComponentToSlot(cluster, topology, supervisorAvailableSlots);

        }

        //assign the remaining system task storm default even scheduler will handle the rest work.
        new EvenScheduler().schedule(topologies, cluster);

    }


}
