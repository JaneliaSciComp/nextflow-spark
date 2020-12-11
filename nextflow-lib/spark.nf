process spark_master {
    container = 'bde2020/spark-master:3.0.1-hadoop3.2'

    input:
    path spark_master_log_dir

    output:
    env URI

    script:
    """
    SPARK_MASTER_LOG="${spark_master_log_dir}"
    echo "Spark master log dir \${SPARK_MASTER_LOG}"
    /spark/bin/spark-class \
    org.apache.spark.deploy.master.Master
    URI=spark://master:7077
    """
}

process spark_worker {
    container = 'bde2020/spark-worker:3.0.1-hadoop3.2'

    input:
    val(u)

    output:
    stdout
    
    script:
    """
    """
}
