ratings  = load 'path' USING PIGSTORAGE('|') AS (userid:Int,var2:chararray)

namelookup = FOREACH ratings GENERATE columns,func(columns)

grouping = GROUP ratings by columntobegroupedby

groupingresult = FOREACH grouping GENERATE group as columntobegroupedby,aggregatefn(columns) as alais

filtering = FILTER groupingresult by alais > 4.0

joineg = JOIN relationA by column1,relationB by column1

join_result = JOIN relationA by (project_id, sequence_id) LEFT OUTER, relationB by (project_id, sequence_id);


C = COGROUP A BY name, B BY name;

ordereg = ORDER joineg by relationb::COLUMNNAME

DUMP ordereg


mean = foreach grp {
    sum = SUM(inpt.amnt);
    count = COUNT(inpt);
    generate group as id, sum/count as mean, sum as sum, count as count;
};


 mean = foreach grp {
    sum = SUM(inpt.amnt);
    count = COUNT(inpt);
    generate FLATTEN(inpt), sum/count as mean, sum as sum, count as count;
};



inpt = load '~/pig_data/pig_fun/input/group.txt' as (amnt:double, id:chararray, c2:chararray);
grp = group inpt by id;
mean = foreach grp {
        sum = SUM(inpt.amnt);
        count = COUNT(inpt);
        generate flatten(inpt), sum/count as avg, count as count;
};
tmp = foreach mean {
    dif = (amnt - avg) * (amnt - avg) ;
     generate *, dif as dif;
};
grp = group tmp by id;
standard_tmp = foreach grp generate flatten(tmp), SUM(tmp.dif) as sqr_sum; 
standard = foreach standard_tmp generate *, sqr_sum / count as variance, SQRT(sqr_sum / count) as standard;


sd using formula : 
var=E(x^2)-(Ex)^2
inpt = load '~/pig_data/pig_fun/input/group.txt' as (amnt:double,  id:chararray, c2:chararray);
grp = group inpt by id;
mean = foreach grp {
    sum = SUM(inpt.amnt);
    sum2 = SUM(inpt.amnt**2);
    count = COUNT(inpt);
    generate flatten(inpt), sum/count as avg, count as count, sum2/count-    (sum/count)**2 as std;
};



3rd highest salary :

A = LOAD 'data.txt' USING PigStorage('\t') AS (name:chararray,salary:int);
B = FOREACH A GENERATE A.Salary;
C = DISTINCT B;
D = ORDER C BY C.$0 DESC;
E = LIMIT D 3;
F = ORDER E BY E.$0 ASC;
G = LIMIT F 1;
H = FILTER A BY (A.Salary = G.$0);


counter in pig:


import com.twitter.elephantbird.pig.util.PigCounterHelper;

PigCounterHelper counterHelper = new PigCounterHelper();
counterHelper.incrCounter(COUNTER_GROUP_NAME, counterName, incrValue);
#https://dzone.com/articles/counters-apache-pigs


key partitioning in pig : 
A = LOAD 'input' USING PigStorage(',') AS (f1,f2);
SPLIT A INTO k1 IF (f1=='key1'), k2 IF (f1=='key2'), k3 IF (f1=='key3');
STORE k1 INTO 'OutputKey1' USING PigStorage();
STORE k2 INTO 'OutputKey2' USING PigStorage();
STORE k3 INTO 'OutputKey3' USING PigStorage();

OR 

REGISTER '/tmp/piggybank.jar';
A = LOAD 'input*' USING PigStorage(',') AS (f1,f2);
STORE A INTO 'output' USING org.apache.pig.piggybank.storage.MultiStorage('output', '0');

OR

A = LOAD 'input_data'; 
B = GROUP A BY $0 PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner PARALLEL 2(no. of reducers);


binning in pig : 
-- foo.pig
ids = load '$INPUT' as (id: int);
ids_with_key = foreach ids generate (id - $MIN) * $BIN_COUNT / ($MAX- $MIN + 1) as bin_id, id;
group_by_id = group ids_with_key by bin_id;
bin_id = foreach group_by_id generate group, flatten(ids_with_key.id);
dump bin_id;



join types : 

joineg = JOIN relationA by column1,relationB by column1

join_result = JOIN relationA by (project_id, sequence_id) LEFT OUTER, relationB by (project_id, sequence_id);

join using 'replicated'/'bloom'/ 'skewed' / 'merge'

replicated : small table copied to all nodes having big table (table shouldd be small)
bloom : bloom filter created from small table is replicated (data to be moved is small irrespective of table size)

skew : Parallel joins are vulnerable to the presence of skew in the underlying data. If the underlying data is sufficiently skewed, load imbalances will swamp any of the parallelism gains. In order to counteract this problem, skewed join computes a histogram of the key space and uses this data to allocate reducers for a given key

merge : helpful when data already sorted

