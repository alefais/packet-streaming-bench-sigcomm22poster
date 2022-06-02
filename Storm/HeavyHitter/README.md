# Compile and Run Heavy Hitter application

## Preliminary steps: configuration
Set the application specific configuration properties in [hh.properties](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/Storm/HeavyHitter/src/main/resources/hh.properties)

## Compilation
Build the application with `maven` as: <br> 
```
mvn -X clean package
```

## Execution
To run the streaming application you can now use the Storm local engine `$STORM_HOME/bin/storm` with parameter `local`, the built `jar` with dependencies and the main application class `HeavyHitter.HeavyHitter`. Heavy Hitter can take some arguments as command line parameters (run it with `help` to see all the available options). It is sufficient to pass all application parameters after the main class name (application entrypoint) during the invocation.

### Execution examples:
* No argument is passed (default configuration is used: all the nodes have parallelism degree equal to 1, the source generation rate is the maximum possible, the input data set is the default one (inside the `data/` directory). See (and modify if you want) all default configuration values in [hh.properties](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/Storm/HeavyHitter/src/main/resources/hh.properties). <br> 
```
$STORM_HOME/bin/storm local target/HeavyHitter-1.0-SNAPSHOT-jar-with-dependencies.jar HeavyHitter.HeavyHitter
```

* Application run passing some parameters: input dataset file, parallelism degree of all operators (order is relevant!), window length and slide, threshold value. <br>
```
$STORM_HOME/bin/storm local target/HeavyHitter-1.0-SNAPSHOT-jar-with-dependencies.jar HeavyHitter.HeavyHitter data/pcap.csv 1 1 4 1 1 2000 100 1500
```