spark-submit --master yarn --deploy-mode cluster \
--py-files project_lib.zip \
--files conf/application.conf,conf/spark.conf,log4j.properties \
-- driver-cores 2 \
-- driver-memory 4G \
-- conf spark.driver.memoryOverhead=1G
project_main.py qa 2025-08-28