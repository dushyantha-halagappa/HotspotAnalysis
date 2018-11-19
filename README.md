# HotspotAnalysis
Solution for hot spot and hot zone analysis.

# Procedure to compile and run:
From the project root folder;

sbt compile

sbt clean assembly

spark-submit path_to_jar_file output_path hotzoneanalysis src/resources/input_file.csv src/resources/input_file.csv hotcellanalysis src/resources/input_file.csv

Above commands are written assuming, SBT, Scala installed on the system, Scala path is set and created alias to spark-submit binary.

