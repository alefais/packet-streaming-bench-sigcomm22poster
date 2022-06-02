# Compile and Run Heavy Hitter application

## Preliminary steps

### Framework configuration
To enable the [parallel execution](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/execution/parallel/) of some operator, you first need to tune some parameters of the Flink framework.

* **Number of task slots:** <br> This value must be configured in the `$FLINK_HOME/conf/flink-conf.yaml` file. The default is `taskmanager.numberOfTaskSlots: 1`. You can set it to match the number of available cores in your machine.

* **Process memory:** <br> There are some values that can be configured in the `$FLINK_HOME/conf/flink-conf.yaml` file. This is [a useful guide](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/mem_setup/) to tune these parameters.

### Application configuration
Set the application specific configuration properties in [hh.properties](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/Flink/HeavyHitter/src/main/resources/hh.properties)


## Compilation
Build the application with `maven` as: <br> 
```
mvn -X clean package
```

## Execution

1. Start Flink local cluster as: <br> 
```
$FLINK_HOME/bin/start-cluster.sh
```

2. To run the streaming application you can now use the Flink local engine `$FLINK_HOME/bin/flink` and pass as arguments the `run` command, the main application class `HeavyHitter.HeavyHitter` and the built `jar` with dependencies. Heavy Hitter can take some arguments as command line parameters (run it with `help` to see all the available options). It is sufficient to pass all parameters after the path of the application jar during the invocation.

3. Check the <b>Flink UI</b> available at `http://localhost:8081`. Also, check logs and output in the `$FLINK_HOME/log/` directory.

4. Stop Flink local cluster as: <br> 
```
$FLINK_HOME/bin/stop-cluster.sh
```

### Execution examples:
* No argument is passed (default configuration is used: all the nodes have parallelism degree equal to 1, the source generation rate is the maximum possible, the input data set is the default one (inside the `data/` directory). See (and modify if you want) all default configuration values in [hh.properties](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/Flink/HeavyHitter/src/main/resources/hh.properties). <br> 
```
$FLINK_HOME/bin/flink run -c HeavyHitter.HeavyHitter target/HeavyHitter-1.0-SNAPSHOT-jar-with-dependencies.jar
```

* Application run passing some parameters: input dataset file, parallelism degree of all operators (order is relevant!), window length and slide, threshold value. <br>
```
$FLINK_HOME/bin/flink run -c HeavyHitter.HeavyHitter target/HeavyHitter-1.0-SNAPSHOT-jar-with-dependencies.jar data/pcap.csv 1 1 4 1 1 2000 100 1500
```