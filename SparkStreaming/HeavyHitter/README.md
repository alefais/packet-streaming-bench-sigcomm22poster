# Compile and Run Heavy Hitter application

## Preliminary steps: configuration
Set the application specific configuration properties in [hh.properties](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/SparkStreaming/HeavyHitter/src/main/resources/hh.properties)

## Compilation
Build the application with `maven` as: <br> 
```
mvn -X clean package
```

## Execution

1. To run the streaming application you can now use the Spark Streaming local engine `$SPARK_HOME/bin/spark-submit` and pass as arguments the main application class `HeavyHitter.HeavyHitter` and the built `jar` with dependencies. Heavy Hitter can take some arguments as command line parameters (run it with `help` to see all the available options). It is sufficient to pass all parameters after the path of the application jar during the invocation.

2. Check the <b>Spark UI</b> available at `http://<url>:4040`.

### Execution example:
* No argument is passed (default configuration is used). See (and modify if you want) all default configuration values in [hh.properties](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/SparkStreaming/HeavyHitter/src/main/resources/hh.properties). <br>
```
$SPARK_HOME/bin/spark-submit --class HeavyHitter.HeavyHitter target/HeavyHitter-1.0-SNAPSHOT-jar-with-dependencies.jar
```