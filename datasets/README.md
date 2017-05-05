This folder contains several datasets that have been tested with JedAI. We have grouped them in two categories: 
1) those suitable for Clean-Clean ER, and 
2) those for Dirty ER. 
Every dataset includes two types of files: 
1) those containing the entity profiles themselves, which are called **entity file**, and 
2) those containing the golden standard (i.e., the ground-truth with the real matches), which are called **ground-truth files**.

Note that every file that is available as a *Java Serialized Object* (**JSO**) can be read using the class DataReader.EntityReader.EntitySerializationReader for entity files or the class DataReader.GroundTruthReader.GtSerializationReader for ground-truth files.

# Dirty ER datasets

| Dataset Name | Entities | Name-Value Pairs | Duplicates | Average NVP per Entity |	Brute-force Comparisons |
File Format | Type | 
|---|---|---|---|---|---|---|---|
| Abt-By	| 2,152	| 4,876	| 1,076	| 2.3	| 2.31E+06| JSO |Real data |
| DBLP-ACM	| 4,910	| 19,626	| 2,224	| 4.0	| 1.21E+07| JSO | Real data |
| DBLP-Scholar	| 63,869| 	208,065	| 2,308	| 3.3	| 2.04E+09| JSO | Real data |
| Amazon-GP	| 4,393	| 14,412| 	1,104	| 3.3| 	9.65E+06| JSO | Real data |
| Movies	| 50,797 |	971,445	| 22,863| 	19.1| 	1.29E+09| JSO | Real data |
| DBPedia	| 3,354,773	| 5.19E+07| 	892,586	| 15.5	| 5.63E+12| JSO | Real data |


# Clean-Clean ER datasets
