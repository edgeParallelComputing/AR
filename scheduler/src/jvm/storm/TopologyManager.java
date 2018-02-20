package storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.File;
import java.io.*;

import storm.CurveFit;

public class TopologyManager {

    public double getTaskInput(String task) {
        return taskMap.get(task).getInputSize();
    }

    public double getTaskOutput(String task) {
        return taskMap.get(task).getOutputSize();
    }

    public double getTaskMemory(String task) {
        return taskMap.get(task).getMemory();
    }

    public double getTaskCpuUtilization(String task) {
        return taskMap.get(task).getCpuUtilization();
    }

    public double getTaskUpBandwidth(String task) {
        return taskMap.get(task).getUpBandwidth();
    }

    public double getTaskDownBanwidth(String task) {
        return taskMap.get(task).getDownBanwidth();
    }

    public double getTaskLatency(String task, double utilization) {
        return taskMap.get(task).getLatency(utilization);
    }


    private final String topologyLogFile;
    private final String topologyName;

    private Map<String, TaskResource> taskMap = new Hashtable<>();

    public ArrayList<String> getTaskList() {
        return new ArrayList<>(taskMap.keySet());
    }

    public TopologyManager(String topologyName, String topologyLogFile) {
        this.topologyName = topologyName;
        this.topologyLogFile = topologyLogFile;
    }

    public void init() throws Exception {

        try {
            for (int i = 5; i < 100; i += 5) {
                String logAddr = this.topologyLogFile + String.valueOf(i) + ".log";
                processTaskLog(logAddr, (double) i);
            }
        } catch (Exception e) {
            System.err.println(e);
        }
        computeLatencyEstimationCurve();
    }

    private void processTaskLog(String logAddr, double utilization) throws Exception {
        try {
            FileReader fileReader =
                    new FileReader(logAddr);

            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.contains("<message>")) {
                    consumeTaskLog(line, utilization);
                }
            }

            bufferedReader.close();

        } catch (FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            logAddr + "'");

        } catch (IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + logAddr + "'");
        }
    }

    private void consumeTaskLog(String line, double utilization) {
        Pattern p = Pattern.compile("\\[([^\\]]+)\\]");
        Matcher m = p.matcher(line);
        List<String> results = new ArrayList<>();
        while (m.find()) {
            results.add(m.group(1));
        }

        String taskName = results.get(0);
        double elapseTime = Double.valueOf(results.get(4));
        double cpuUtilization = Double.valueOf(results.get(5));
        double memory = Double.valueOf(results.get(6));
        double input = Double.valueOf(results.get(7));
        double output = Double.valueOf(results.get(8));
        double downloadBw = input * 10;
        double upLoadBw = output * 10;

        TaskResource taskResource = taskMap.get(taskName);
        if (taskResource == null) {
            taskResource = new TaskResource(taskName);
            taskMap.put(taskName, taskResource);
            taskResource.setInputSize(input);
            taskResource.setOutputSize(output);
            taskResource.setcpuUtilization(cpuUtilization);
            taskResource.setMemory(memory);
            taskResource.setUpBandwidth(upLoadBw);
            taskResource.setDownBanwidth(downloadBw);
        }

        if (taskResource.latencyUtilizationListMap.get(utilization) == null) {
            List<Double> latencyList = new ArrayList<>();
            latencyList.add(elapseTime);
            taskResource.latencyUtilizationListMap.put(utilization, latencyList);
        }

        taskResource.latencyUtilizationListMap.get(utilization).add(elapseTime);

    }

    /*
     * 1-> loop over the task list and calculate the average task latency under each utilization setup
     * 2-> given the sampling latency under different utilizations, we fit them into a curve and use it to estimate the latency under any utilization
     */
    private void computeLatencyEstimationCurve() {
        for (Map.Entry<String, TaskResource> entry : taskMap.entrySet()) {
            TaskResource taskResource = entry.getValue();
            for (Map.Entry<Double, List<Double>> entry1 : taskResource.latencyUtilizationListMap.entrySet()) {
                double utilization = entry1.getKey();
                List<Double> latencyList = entry1.getValue();
                double sum = 0;
                for (int i = 0; i < latencyList.size(); i++) {
                    sum += latencyList.get(i);
                }
                double averageLatency = sum / (double) latencyList.size();
                taskResource.setLatencyUtilization(utilization, averageLatency);
            }
            taskResource.curveFit();
        }
    }


    private class TaskResource {

        private final String taskName;
        //unit: MByte
        private double memory = 0.0;
        private double inputSize = 0.0;
        private double outputSize = 0.0;
        private double upBandwidth = 0.0;
        private double downBanwidth = 0.0;
        private double cpuUtilization = 0.0;

        public Map<Double, List<Double>> latencyUtilizationListMap = new Hashtable<>();
        private Map<Double, Double> latecnyUtilizationMap = new Hashtable<>();
        private CurveFit latencyCurve;

        public void curveFit() {
            List<Double> utilization = new ArrayList<>();
            List<Double> latency = new ArrayList<>();
            for (Map.Entry<Double, Double> entry : latecnyUtilizationMap.entrySet()) {
                System.out.println(entry.getKey() + " " + entry.getValue());
                utilization.add(entry.getKey());
                latency.add(entry.getValue());
            }
            System.out.println(" \n\n");
            latencyCurve = new CurveFit(utilization, latency);
            latencyCurve.init(3);
        }

        public double getEstimatedLatency(double utilization) {
            if (latencyCurve == null) {
                return 0;
            }
            return latencyCurve.predict(utilization);
        }

        public TaskResource(String task) {
            taskName = task;
        }

        public void setMemory(double val) {
            memory = val;
        }

        public void setInputSize(double val) {
            inputSize = val;
        }

        public void setOutputSize(double val) {
            outputSize = val;
        }

        public void setUpBandwidth(double val) {
            upBandwidth = val;
        }

        public void setDownBanwidth(double val) {
            downBanwidth = val;
        }

        public void setcpuUtilization(double val) {
            cpuUtilization = val;
        }

        public void setLatencyUtilization(double utilization, double latency) {
            latecnyUtilizationMap.put(utilization, latency);
        }

        public double getMemory() {
            return memory;
        }

        public double getInputSize() {
            return inputSize;
        }

        public double getOutputSize() {
            return outputSize;
        }

        public double getUpBandwidth() {
            return upBandwidth;
        }

        public double getDownBanwidth() {
            return downBanwidth;
        }

        public double getCpuUtilization() {
            return cpuUtilization;
        }

        public double getLatency(double utilization) {
            return getEstimatedLatency(utilization);
        }
    }

    public static void main(String args[]) throws Exception {
        String topologyName = "AR";
        String topologyLogFile = "/Users/wuyang/ar/utilization/node21-1/allLog_";
        TopologyManager topologyManager = new TopologyManager(topologyName, topologyLogFile);
        try {
            topologyManager.init();
        } catch (Exception e) {
            System.err.println(e);
        }

        for(int i = 0; i < 100; i++){
            //System.out.println(topologyManager.getTaskLatency("VideoSpout", i));
            //System.out.println(topologyManager.getTaskLatency("RectificationAndPartitionBolt", i));
        }
//        System.out.println(topologyManager.getTaskInput("rectificationBolt"));
//        System.out.println(topologyManager.getTaskMemory("partitionBolt"));
//        System.out.println(topologyManager.getTaskLatency("disparityBolt", 42));
    }
}
