# This is a Work in Progress
## How to run?
1. `docker-compose up`. This will start the control center at http://localhost:9021 and the schema registry at http://localhost:8081/subjects
2. To generate java classes from the `*.avsc` files execute `sbt avroGenerate` or `sbt compile` this will generate the files under `target\scala-2.13\src_managed\main\compiled_avro`. Make sure this directory is also on your classpath.