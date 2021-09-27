# spark-project
A simple scala-based spark application

## Quick start
1. Compile and build JAR file
```shell script
sbt compile assembly
```

2. Upload JAR to S3

3. Run Spark
```shell script
spark-submit --class jhyoo1220.app.GenerateRandomUsers {JAR path} {arguements}
```

## How to run code formatter
* scalafmt
```shell script
sbt scalafmt
```

* scalastyle
```shell script
sbt scalastyle
```
