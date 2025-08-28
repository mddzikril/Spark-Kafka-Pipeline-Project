spark-submit --master yarn --deploy-mode cluster \
--py-files project_lib.zip \
--files conf/application.conf,conf/spark.conf,log4j.properties \
project_main.py qa 2025-08-28