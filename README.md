# Java gEneric DAta Integration (JedAI) Toolkit
JedAI constitutes an open source, high scalability toolkit that offers out-of-the-box solutions for any data integration task, e.g., Record Linkage, Entity Resolution and Link Discovery. 

At the core of JedAI lies a set of *domain-independent*, *state-of-the-art* techniques that apply to both RDF and relational data. These techniques rely on an approximate, *schema-agnostic* functionality based on *blocking* for high scalability. 

In more detail, JedAI supports the following functionalities grouped into 5 modules:

### Data Reading 
It transforms the input data into a list of entity profiles. An entity is a uniquely identified set of name-value pairs (e.g., an RDF resource with its URI as identifier and its set of predicates and objects as name-value pairs). 

The following formats are currently supported:
 1) CSV 
 2) RDF (any format, including XML, OWL)
 3) SQL (mySQL, PostgreSQL)
 
The next version will add support for more formats: SPARQL endpoints, JSON, MongoDB, Oracle and SQL Server.
  
### Block Building 
It clusters entities into blocks in a lazy manner that relies on unsupervised blocking keys: every token in an attribute value forms a key. Blocks are then extracted, possibly using a transformation, based on its equality or on its similarity with other keys.

The following methods are currently supported:
 1) Standard/Token Blocking
 2) Attribute Clustering
 3) Sorted Neighborhood
 4) Extended Sorted Neighborhood
 5) Q-Grams Blocking
 6) Extended Q-Grams Blocking
 7) Suffix Arrays Blocking
 8) Extended Suffix Arrays Blocking
  
For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/vol9/p312-papadakis.pdf).  

### Block Cleaning
Its goal is to clean a set of blocks from unnecessary comparisons, which can be either *redundant* (i.e., repeated comparisons that have already been executed in a previously examined block) or *superfluous* (i.e., comparisons that involve non-matching entities). Its methods operate on the coarse level of entire blocks.

The following methods are currently supported:
 1) Block Filtering
 2) Block Scheduling
 3) Size-based Block Purging
 4) Size-based Block Purging

All methods are optional, but complementary with each other and can be used in combination. For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf).  

### Comparison Cleaning
Similar to Block Cleaning, this step aims to clean a set of blocks from both redundant and superfluous comparisons. Unlike Block Cleaning, its methods operate on the finer granularity of individual comparisons. 

The following methods are currently supported:
 1) Comparison Propagation
 2) Cardinality Edge Pruning (CEP)
 3) Cardinality Node Pruning (CNP)
 4) Weighed Edge Pruning (WEP)
 5) Weighed Node Pruning (WNP)
 6) Reciprocal Cardinality Node Pruning (ReCNP)
 7) Reciprocal Weighed Node Pruning (ReWNP)

Most of these methods are Meta-blocking techniques. All methods are optional, but competive, in the sense that only one of them can part of an ER workflow. For more details on the functionality of these methods, see [here](http://www.sciencedirect.com/science/article/pii/S2214579616300168).  

### Entity Matching
It compares pairs of entity profiles, associating every pair with a similarity in [0,1]. Its output comprises the *similarity graph*, i.e., an undirected, weighted graph where the nodes correspond to entities and the edges connect pairs of compared entities. 

The following schema-agnostic methods are currently supported:
1) [Group Linkage](http://pike.psu.edu/publications/icde07.pdf), 
2) Profile Matcher, which aggregates all attributes values in an individual entity into a textual representation.

Both methods can be combined with the following representation models.
 1) character n-grams (n=2, 3 or 4)
 2) character n-gram graphs (n=2, 3 or 4)
 3) token n-grams (n=1, 2 or 3)
 4) token n-gram graphs (n=1, 2 or 3)

For more details on the functionality of these bag and graph models, see [here](https://link.springer.com/article/10.1007%2Fs11280-015-0365-x).

The bag models can be combined with the following similarity measures, using term-frequency weights: 
   1) Cosine similarity 
   2) Jaccard similarity 
   3) Generalized Jaccard similarity 
   4) Enhanced Jaccard similarity
   
The graph models can be combined with the following graph similarity measures:
   1) Containment similarity 
   2) Normalized Value similarity 
   3) Value similarity 
   4) Overall Graph similarity

### Entity Clustering
It takes as input the similarity graph produced by Entity Matching and partitions it into a set of equivalence clusters, with every cluster corresponding to a distinct real-world object.

The following domain-independent methods are currently supported:
1) Center Clustering
2) Connected Components Clustering
3) Cut Clustering
4) Markov Clustering
5) Merge-Center Clustering
6) Ricochet SR Clustering

For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/2/vldb09-1025.pdf). 

### Consortium

JEDAI is a collaboration project involving the following partners:
* [Department of Informatics and Telecommunications, University of Athens](http://www.di.uoa.gr),
* [Software and Knowledge Engineering Lab, National Center for Scientific Research "Demokritos"](https://www.iit.demokritos.gr/skel) ,
* [Science-For-You not-for-profit company](http://www.scify.gr/site/en) 
* [LIPADE, Paris Descartes University](http://lipade.mi.parisdescartes.fr)
* [Department of Computer Science, University of Leuven](https://wms.cs.kuleuven.be/cs/english)
