# Compile and Run Heavy Hitter application

## Compilation
In this project, the [alglib](https://www.alglib.net/statistics/descriptive.php) library has been used and needs to be built before compiling the application. The files can be downloaded from the official [ALGLIB Free Edition web page](https://www.alglib.net/download.php). For the user convenience, the required library files have been included in this project directory, and a Makefile has been provided to the user, to simplify the compilation. After the library has been built, you can  proceed with compiling the application. The required steps to follow are:
```
make alglib
make all
```

To clean up the files resulting from the application build process run:
```
make clean
```

## Execution
The application can be run as:
```
./hh.out      [ -i input_file ]
                [ -p nSource,nFlowId,nMap,nReduce,nDetector,nSink ]
                [ -b batch_size ]
                [ -w winLen (ms) ]
                [ -s winSlide (ms) ]
                [ -c (enables chaining) ]
```

### Execution example:
* The arguments passed define the input file, the parallelism degree to use for each streaming operator in the graph, the batch size, the window length and slide, and the threshold.
```
./hh.out -i dump.pcap -p 2,1,6,2,2 -b 32 -w 2000 -s 100 -t 1500 -c
```