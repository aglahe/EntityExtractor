REGISTER /usr/lib/zookeeper/zookeeper-3.4.5-cdh5.2.0.jar
REGISTER /usr/lib/hbase/hbase-client-0.98.6-cdh5.2.0.jar
REGISTER ../../../target/loader-0.0.1-SNAPSHOT.jar

--Load the JSON
--json = LOAD '/user/aglahe/input/ebola_scrape.json' USING JsonLoader('title:chararray, url:chararray, cleaned_text:chararray');
json = LOAD '../../../../data/ebola_scrape.json' USING JsonLoader('title:chararray, url:chararray, cleaned_text:chararray');

-- Generate id
json_with_ids = FOREACH json GENERATE com.datatactics.memex.udf.UddiGenerator(title) AS id, *;

--dump json_with_ids;
STORE json_with_ids INTO 'hbase://aaron-ee' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('orig:title orig:url orig:cleaned_text');
