compile:
	mvn clean package
clean:
	mvn clean
serial:
	/usr/local/storm/bin/storm jar target/storm-starter-1.1.1.jar storm.topology.SerialReconstructionTopology

parallel:
	/usr/local/storm/bin/storm jar target/storm-starter-1.1.1.jar storm.topology.ParallelReconstructionTopology

nonPipe:
	/usr/local/storm/bin/storm jar target/storm-starter-1.1.1.jar storm.topology.NonPipelineTopology

serialC:
	/usr/local/storm/bin/storm jar target/storm-starter-1.1.1.jar storm.topology.SerialReconstructionTopology AR

parallelC:
	/usr/local/storm/bin/storm jar target/storm-starter-1.1.1.jar storm.topology.MergeParallelReconstructionTopology AR

nonPipeC:
	/usr/local/storm/bin/storm jar target/storm-starter-1.1.1.jar storm.topology.NonPipelineTopology AR
