./main.nf -profile localdocker \
	--runtime_opts "-u $(id -u):$(id -g) -v $PWD/local:$PWD/local" \
	--workers 1 \
	--worker_cores 5 \
	--gb_per_core 4 \
	--driver_cores 1 \
	--driver_memory 2g \
	--spark_work_dir "$PWD/local" \
	--app_jar "$PWD/local/app.jar" \
	--app_main org.janelia.colormipsearch.cmd.SparkMainEntry \
	--app_args \
	"searchFromJSON, \
	-m,$PWD/local/testData/mask.json, \
	-i,$PWD/local/testData/mcfoTargets.json, \
        --mirrorMask, \
        --maskThreshold,100, \
        --dataThreshold,100, \
        --xyShift,0, \
        --pctPositivePixels,2, \
        --pixColorFluctuation,2, \
        -od $PWD/local/testData/cdsresults.test"