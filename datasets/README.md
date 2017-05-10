This folder contains several datasets that have been tested with JedAI. We have grouped them in two categories: 
1) those suitable for Clean-Clean ER, and 
2) those sutable for Dirty ER. 

Every dataset includes two types of files: 
1) those containing the entity profiles themselves, which are called **entity files**, and 
2) those containing the golden standard (i.e., the ground-truth with the real matches), which are called **groundtruth files**.

Note that every file that is available as a *Java Serialized Object* (**JSO**) can be read using the class DataReader.EntityReader.EntitySerializationReader for entity files or the class DataReader.GroundTruthReader.GtSerializationReader for ground-truth files. See [this class](https://github.com/scify/JedAIToolkit/blob/master/jedai-core/tests/GeneralExamples/DirtyErDatasetStatistics.java) for an example.

# Dirty ER datasets

| Dataset Name | Entities | Name-Value Pairs | Duplicates | Average NVP per Entity |	Brute-force Comparisons |
File Format | Data Origin | 
|---|---|---|---|---|---|---|---|
|Restaurant	|864	|4,319|	112|	5.0|	3.73E+05|JSO ([entity file](dirtyERfiles/restaurantProfiles), [groundtruth file](dirtyERfiles/restaurantIdDuplicates)) |Real data |
|Census	|841	|3,913	|344|	4.7|	3.53E+05|JSO ([entity file](dirtyERfiles/censusProfiles), [groundtruth file](dirtyERfiles/censusIdDuplicates)) |Real data |
|Cora|	1,295	|7,166|	17,184|	5.5|8.38E+05|JSO ([entity file](dirtyERfiles/coraProfiles), [groundtruth file](dirtyERfiles/coraIdDuplicates)) |Real data |
|CdDb	|9763	|173,309	|299|	17.8	|4.77E+07|JSO ([entity file](dirtyERfiles/cddbProfiles), [groundtruth file](dirtyERfiles/cddbIdDuplicates)) |Real data |
| Abt-By	| 2,152	| 4,876	| 1,076	| 2.3	| 2.31E+06| JSO ([entity file](dirtyERfiles/abtBuyProfiles), [groundtruth file](dirtyERfiles/abtBuyIdDuplicates)) |Real data |
| DBLP-ACM	| 4,910	| 19,626	| 2,224	| 4.0	| 1.21E+07| JSO ([entity file](dirtyERfiles/dblpAcmProfiles), [groundtruth file](dirtyERfiles/dblpAcmIdDuplicates)) | Real data |
| DBLP-Scholar	| 63,869| 	208,065	| 2,308	| 3.3	| 2.04E+09| JSO ([entity file](dirtyERfiles/dblpScholarProfiles), [groundtruth file](dirtyERfiles/dblpScholarIdDuplicates)) | Real data |
| Amazon-GP	| 4,393	| 14,412| 	1,104	| 3.3| 	9.65E+06| JSO ([entity file](dirtyERfiles/amazonGpProfiles), [groundtruth file](dirtyERfiles/amazonGpIdDuplicates)) | Real data |
| Movies	| 50,797 |	971,445	| 22,863| 	19.1| 	1.29E+09| JSO ([zipped entity file](dirtyERfiles/moviesProfiles.zip), [groundtruth file](dirtyERfiles/moviesIdDuplicates)) | Real data |
<!---| DBPedia	| 3,354,773	| 5.19E+07| 	892,586	| 15.5	| 5.63E+12| JSO | Real data |-->


# Clean-Clean ER datasets

| Dataset Name | D1 Entities| D2 Entities | D1 Name-Value Pairs	| D2 Name-Value Pairs	| Duplicates | Average NVP per Entity	|
Brute-force Comparisons | File Format | Data Origin | 
|---|---| ---| ---| ---| ---|---| ---| ---| ---| 
|Abt-Buy	|1,076|	1,076|	2,568	|2,308|	1,076	|2.4|	1.16E+06|JSO ([Abt entity file](cleanCleanERfiles/abtProfiles), [Buy entity file](cleanCleanERfiles/buyProfiles), [groundtruth file](cleanCleanERfiles/abtBuyIdDuplicates))|Real data |
|DBLP-ACM|	2,616	|2,294	|10,464	|9,162|	2,224	|4.0|	6.00E+06|JSO ([DBLP entity file](cleanCleanERfiles/dblpProfiles), [ACM entity file](cleanCleanERfiles/acmProfiles), [groundtruth file](cleanCleanERfiles/dblpAcmIdDuplicates))|Real data |
|DBLP-Scholar|	2,516	|61,353|	10,064	|198,001|	2,308|	4.0|	1.54E+08|JSO ([DBLP entity file](cleanCleanERfiles/dblpProfiles2), [Scholar entity file](cleanCleanERfiles/scholarProfiles), [groundtruth file](cleanCleanERfiles/dblpScholarIdDuplicates))|Real data |
|Amazon-Google Products|	1,354	|3,039	|5,302	|9,110	|1,104	|3.9|	4.11E+06|JSO ([Amazon entity file](cleanCleanERfiles/amazonProfiles), [GP entity file](cleanCleanERfiles/gpProfiles), [groundtruth file](cleanCleanERfiles/amazonGpIdDuplicates))|Real data |
|Movies	|27,615	|23,182|	155,436	|816,009	|22,863|	5.6|	6.40E+08|JSO ([IMDB entity file](cleanCleanERfiles/imdbProfiles), [DBPedia entity file](cleanCleanERfiles/dbpediaProfiles.zip), [groundtruth file](cleanCleanERfiles/moviesIdDuplicates))|Real data |
<!---|DBPedia|	1,190,733	|2,164,040	|1.69E+07	|3.50E+07	|892,586	|14.2	|2.58E+12|JSO |Real data |-->
