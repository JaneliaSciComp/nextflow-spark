./main.nf -profile lsf \
        --runtime_opts "-e -B $PWD/local -B $PWD/examples -B /nrs/jacs/jacsData/filestore" \
        --lsf_opts "-P scicompsoft -W 30" \
        --workers 3 \
        --worker_cores 4 \
        --executor_cores 4 \
        --gb_per_core 15 \
        --driver_cores 1 \
        --driver_memory 10g \
        --driver_deploy_mode client \
        --spark_conf $PWD/examples/spark-conf \
        --spark_work_dir "$PWD/local" \
        --app $PWD/local/app.jar \
        --app_main org.janelia.colormipsearch.cmd.SparkMainEntry \
        --app_args \
        "searchFromJSON \
        -m $PWD/local/testData/mask-2.json \
        -i $PWD/local/testData/sgal4Targets.json \
        --mirrorMask \
        --maskThreshold 100 \
        --dataThreshold 100 \
        --xyShift 2 \
        --pctPositivePixels 2.0 \
        --pixColorFluctuation 2 \
        -od $PWD/local/testData/cdsresults.test"
