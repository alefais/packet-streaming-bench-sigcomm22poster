[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![Hits](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2Falefais%2Fpacket-streaming-bench-sigcomm22poster&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=Hits&edge_flat=false)](https://hits.seeyoufarm.com)

# Heavy hitter detection - Stream Benchmark

This repository includes the implementation of a heavy hitter detection network traffic query. Four different Stream Processing Systems (SPSs) have been used in this benchmark.

| **Framework** | **Version used** | **Source code** |
| :--- | :---: | :---: |
|**[Apache Flink](https://flink.apache.org/)**|[1.14.4 (for Scala 2.12)](https://flink.apache.org/news/2022/03/11/release-1.14.4.html)|[Link](https://github.com/apache/flink)|
|**[Apache Spark Streaming](https://spark.apache.org/streaming/)**|[3.2.1](https://spark.apache.org/releases/spark-release-3-2-1.html)|[Link](https://github.com/apache/spark)|
|**[Apache Storm](https://storm.apache.org/)**|[2.4.0](https://storm.apache.org/2022/03/25/storm240-released.html)|[Link](https://github.com/apache/storm)|
|**[WindFlow](https://paragroup.github.io/WindFlow/)**|[3.0.0](https://github.com/ParaGroup/WindFlow/releases/tag/3.0.0)|[Link](https://github.com/ParaGroup/WindFlow)|

The code released as open source is part of our research aiming to shed some lights on the best streaming engine for network traffic analysis.

# Conference Poster at [SIGCOMM '22 Posters and Demos](https://conferences.sigcomm.org/sigcomm/2022/cf-posters.html)

If you are interested, have a look at our poster ["Mind the Cost of Telemetry Data Analysis"](https://github.com/alefais/packet-streaming-bench-sigcomm22poster/blob/master/Fais-sigcomm22posters-poster63-A0.pdf) (A0 size).

# How to cite

If our work reveals to be useful for your research, we kindly ask you to give credit to our effort by citing the following paper:
```
@inproceedings{TelemetryStreamBench,
    author = {Fais, Alessandra and Antichi, Gianni and Giordano, Stefano and Lettieri, Giuseppe and Procissi, Gregorio},
    title = {{Mind the Cost of Telemetry Data Analysis}},
    year = {2022},
    booktitle = {Special Interest Group on Data Communication (SIGCOMM)},
    publisher = {Association for Computing Machinery},
    address = {New York, NY, USA},
    abstract = {Data Stream Processing engines are emerging as a promising solution to efficiently process a continuous amount of telemetry information. In this poster, we compare four of them: Storm, Flink, Spark and WindFlow. The aim is to shed some lights on the best streaming engine for network traffic analysis.},
    keywords = {streaming analysis, measurement, data center, programmability},
    location = {Amsterdam, Netherlands},
    series = {SIGCOMM '22}
}
```
<!-- 
    url = {https://doi.org/10.1145/3546037.3546052},
    doi = {10.1145/3546037.3546052}, 
-->

# Contributors

The main developer and maintainer of this repository is [Alessandra Fais](mailto:alessandra.fais@phd.unipi.it).
