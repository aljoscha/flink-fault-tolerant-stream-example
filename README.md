# Fault-Tolerant Streaming with Flink

This is a demo to show how Flink can deal with stateful streaming jobs and
fault-tolerance. The idea is to have an analysis job with a stateful counter.
The job is started on a cluster with two TaskManagers, then the TaskManagers
are shot down to see how Flink reacts.

These are the steps to take:

 1. Build Flink 0.10-SNAPSHOT: This is required to get the latest
    checkpoint/recovery code. If you have a checkout of the Flink repository
    a quick `mvn clean install -DskipTests` should do the trick.

 2. Follow the *Hello Samza* example from here:
   [hello samza](http://samza.apache.org/startup/hello-samza/0.9/).
    We use this to get
    Kafka up and running and also to parse the wikipedia edit stream and
    put it into a Kafka topic.

 3. Package the example using `mvn package`.

 4. Start local Flink cluster with two TaskManagers. This can be done using:

    ```bash
    bin/start-cluster-streaming.sh
    rm /tmp/my-taskmanager.pid && bin/taskmanager.sh start streaming
    ```

 5. Start the example job on the Flink cluster:

    ```bash
    bin/flink run -c com.dataartisans.streamexample.ScalaJobCheatSheet /path/to/example.jar
    ```

 6. Get PIDs of running TaskManagers using `jps`. Shoot down one TaskManager.
    The job fails and recovers you hit the right one, if not, start another
    TaskManager using the earlier command and repeat with the other TaskManager
    PID.

    ```bash
    jps
    kill -KILL <taskmanager-pid>
    ```

