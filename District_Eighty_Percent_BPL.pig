#Register exported jar which contains UDF for filtering Eighty percent bpl Objectives
REGISTER /home/acadgild/HadoopProject2/filterUDF.jar;

#Register Jar required for using XML Loader
REGISTER /home/acadgild/HadoopProject2/piggybank.jar;


#Register Jar required to load data into Hbase Table
REGISTER /usr/local/hbase/lib/hbase-common-0.98.14-hadoop2.jar;
REGISTER /usr/local/hbase/lib/hbase-client-0.98.14-hadoop2.jar; 
REGISTER /usr/local/hbase/lib/hbase-server-0.98.14-hadoop2.jar; 
REGISTER /usr/local/hbase/lib/hbase-protocol-0.98.14-hadoop2.jar;
REGISTER /usr/local/hbase/lib/htrace-core-2.04.jar; 
REGISTER /usr/local/hbase/lib/zookeeper-3.4.6.jar; 
REGISTER /usr/local/hbase/lib/guava-12.0.1.jar;

#Deine XPath function to use for parsing xml files
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

#Load the data from xml using XML loader
row =  LOAD '/home/acadgild/HadoopProject2/StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (rowElement:chararray);

#Get all the xml elements into columns which would get stored into relation elements
elements = FOREACH row GENERATE 
XPath(rowElement, 'row/State_Name') AS State_Name,
XPath(rowElement, 'row/District_Name') AS District_Name,
XPath(rowElement,'row/Project_Objectives_IHHL_BPL') AS Project_Objectives_IHHL_BPL,
XPath(rowElement,'row/Project_Objectives_IHHL_APL') AS Project_Objectives_IHHL_APL,
XPath(rowElement,'row/Project_Objectives_IHHL_TOTAL') AS Project_Objectives_IHHL_TOTAL,
XPath(rowElement,'row/Project_Objectives_SCW') AS Project_Objectives_SCW,
XPath(rowElement,'row/Project_Objectives_School_Toilets') AS Project_Objectives_School_Toilets,
XPath(rowElement,'row/Project_Objectives_Anganwadi_Toilets') AS Project_Objectives_Anganwadi_Toilets,
XPath(rowElement,'row/Project_Objectives_RSM') AS Project_Objectives_RSM,
XPath(rowElement,'row/Project_Objectives_PC') AS Project_Objectives_PC,
XPath(rowElement,'row/Project_Performance-IHHL_BPL') AS Project_Performance_IHHL_BPL,
XPath(rowElement,'row/Project_Performance-IHHL_APL') AS Project_Performance_IHHL_APL,
XPath(rowElement,'row/Project_Performance-IHHL_TOTAL') AS Project_Performance_IHHL_TOTAL,
XPath(rowElement,'row/Project_Performance-SCW') AS Project_Performance_SCW,
XPath(rowElement,'row/Project_Performance-School_Toilets') AS Project_Performance_School_Toilets,
XPath(rowElement,'row/Project_Performance-Anganwadi_Toilets') AS Project_Performance_Anganwadi_Toilets,
XPath(rowElement,'row/Project_Performance-RSM') AS Project_Performance_RSM,
XPath(rowElement,'row/Project_Performance-PC') AS Project_Performance_PC ;

#Use UDF created to filter the districts which have reached 80% of objectives of BPL cards.
filterEightyPercent = FILTER elements by FilterDistrictUdf.FilterDistrict(*);


#Store the result into hbase table Statewise_Eighty_percent_bpl_obj

STORE filterEightyPercent INTO 'hbase://Statewise_Eighty_percent_bpl_obj' USING  org.apache.pig.backend.hadoop.hbase.HBaseStorage('state_data:State_Name,state_data:District_Name,
state_data:Project_Objectives_IHHL_BPL,state_data:Project_Objectives_IHHL_APL,
state_data:Project_Objectives_IHHL_TOTAL,state_data:Project_Objectives_SCW,
state_data:Project_Objectives_School_Toilets,state_data:Project_Objectives_Anganwadi_Toilets,
state_data:Project_Objectives_RSM,state_data:Project_Objectives_PC,
state_data:Project_Performance_IHHL_BPL,state_data:Project_Performance_IHHL_APL,
state_data:Project_Performance_IHHL_TOTAL,state_data:Project_Performance_SCW,
state_data:Project_Performance_School_Toilets,state_data:Project_Performance_Anganwadi_Toilets,
state_data:Project_Performance_RSM,state_data:Project_Performance_PC');

# Store the result into hdfs location from where we can export the results to mysql using sqoop job
STORE filterEightyPercent into 'hdfs://localhost:9000/bpl80percentOutput' using PigStorage(',');
