#!/bin/bash

yarn jar hbase-ner/target/hbase-ner-0.0.1-SNAPSHOT.jar com.datatactics.memex.extractor.ExtractorDriver -conf /etc/hbase/conf/hbase-site.xml
