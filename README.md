# Java gEneric DAta Integration (JEDAI) Toolkit
An open source, high scalability toolkit suitable for any data integration task, e.g., Record Linkage, Entity Resolution and Link Discovery.

JEDAI comprises a set of *domain-independent*, *state-of-the-art* techniques that apply to any domain. At their core lies an approximate, *schema-agnostic* functionality based on *blocking* for high scalability. In more detail, it supports the following functionalities grouped into 5 modules:

### Data Reading 
It transforms the input data into a list of entity profiles. An entity is a uniquely identified sets of name-value pairs. 

The following formats are supported:
 * CSV 
 * RDF (any format, including XML)
 * SQL (mySQL, PostgreSQL)
 * to be added: JSON, MongoDB, Oracle and SQL Server
  
### Block Building 
It clusters entities into blocks in a lazy manner that relies on unsupervised blocking keys: every token in an attribute value forms a key. Blocks are then extracted, possibly using a transformation, based on its equality or on its similarity with other keys.

The following methods are supported:
 * Standard/Token Blocking
 * Attribute Clustering
 * (Extended) Sorted Neighborhood
 * (Extended) Q-Grams Blocking
 * (Extended) Suffix Arrays Blocking
 * to be added: URI Semantics blocking
  
For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/vol9/p312-papadakis.pdf).  

### Block Processing
Its goal is to clean a set of blocks from unnecessary comparisons, which can be either *redundant* (i.e., repeated comparisons that have already been executed in a previously examined block) or *superfluous* (i.e., they involve non-matching entities).
The implemented methods are grouped into *block-refinement* ones, which operate on the coarse level of entire blocks, and *comparison-refinement* ones, which operate on the fine level of individual comparisons. The latter category mostly involves Meta-blocking methods.

The following methods are supported:
* Block-refinement methods
 *  Block Filtering
 *  Block Scheduling
 *  Block Purging
   * Size-based
    * Comparison-based
   
* Comparison-refinement methods
 * Comparison Propagation
 * Cardinality Edge Pruning (CEP)
 * Cardinality Node Pruning (CNP)
 * Weighed Edge Pruning (WEP)
 * Weighed Node Pruning (WNP)
 * Reciprocal Cardinality Node Pruning (ReCNP)
 * Reciprocal Weighed Node Pruning (ReWNP)

For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf).  

### Entity Matching

It compares pairs of entity profiles, associating every pair with a similarity in [0,1].

The following schema-agnostic methods are supported:
* [Group Linkage](http://pike.psu.edu/publications/icde07.pdf), 
* Profile Matcher, which aggregates all attributes values in an individual entity into a textual representation, based on one of the following bag and graph models:
 * character n-grams (n=2,3 or 4)
 * character n-gram graphs (n=2,3 or 4)
 * token n-grams (n=1,2 or 3)
 * token n-gram graphs (n=1, 2 or 3)
   
  The bag models can be combined with the following similarity measures, using term-frequency weights: 
   * Cosine similarity 
   * Jaccard similarity 
   * Generalized Jaccard similarity 
   * Enhanced Jaccard similarity
   
   The graph models can be combined with the following graph similarity measures:
   * Containment similarity 
   * Normalized Value similarity 
   * Value similarity 
   * Overall Graph similarity

### Entity Clustering

It uses the similarities produced by Entity Matching to create the *similarity graph*, i.e., an undirected, weighted graph where the nodes correspond to entities and the edges connect pairs of compared entities. The similarity graph is then partitioned into a set of equivalence clusters, with every cluster corresponding to a distinct real-world object.

The following domain-independent methods are currently supported:
* Center Clustering
* Connected Components Clustering
* Cut Clustering
* Markov Clustering
* Merge-Center Clustering
* Ricochet SR Clustering

For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/2/vldb09-1025.pdf). 


### Consortium

JEDAI is a collaboration project involving the following partners:
* [Department of Informatics and Telecommunications, University of Athens](http://www.di.uoa.gr),
* [Software and Knowledge Engineering Lab, National Center for Scientific Research "Demokritos"](https://www.iit.demokritos.gr/skel) ,
* [Science-For-You not-for-profit company](http://www.scify.gr/site/en) and, 
* [LIPADE, Paris Descartes University](http://lipade.mi.parisdescartes.fr)
