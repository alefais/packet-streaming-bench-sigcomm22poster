# Compile and Run Heavy Hitter

## Compilation
First you need to build the [alglib](https://www.alglib.net/statistics/descriptive.php) library (the required files are included in the project). Next you can build the application. Follow these steps:
```
$ cd HeavyHitter/
$ make alglib
$ make all
```

To clean up the files resulting from the application build process run:
```
$ make clean
```

## Execution
Run the application as:
```
$ ./hh.out      [ -i input_file ]
                [ -p nSource,nFlowId,nMap,nReduce,nDetector,nSink ]
                [ -b batch_size ]
                [ -w winLen (ms) ]
                [ -s winSlide (ms) ]
                [ -c (enables chaining) ]
```

Example:
```
$ ./hh.out -i dump.pcap -p 2,1,6,2,2 -b 32 -w 2000 -s 100 -t 1500 -c
```