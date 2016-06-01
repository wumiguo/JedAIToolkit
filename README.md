# Java gEneric DAta Integration (JEDAI) Toolkit
An open source, high scalability toolkit suitable for any data integration task, e.g., Record Linkage, Entity Resolution and Link Discovery.

JEDAI comprises a set of *generic*, *state-of-the-art* techniques that apply to any domain. At their core lies an approximate, *schema-agnostic* functionality based on *blocking* for high scalability. In more detail, it supports the following functionalities grouped into 5 modules:

### Data Reading 
It trasnforms the input data into a list of entity profiles. An entity is a uniquely identified sets of name-value pairs. 

The following formats are supported:
 * CSV data
 * RDF data (any format)
 * SQL data (mySQL, PostgreSQL)
 * to be added: JSON, MongoDB
  
### Block Building 
It clusters entities into blocks in a lazy manner that relies on unsupervised blocking keys: every token in an attribute value forms a key. Blocks are then extracted, possibly using a transformation, based on its equality or on its similarity with other keys.

The following methods are supported:
 * Standard/Token Blocking
 * Attribute Clustering
 * (Extended) Sorted Neighborhood
 * (Extended) Q-Grams Blocking
 * (Extended) Suffix Arrays Blocking
 * to be added: URI Semantics blocking

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

JEDAI is a collaboration project involving the following partners:
* [Department of Informations and Telecommunications, University of Athens](http://www.di.uoa.gr),
* [Software and Knowledge Engineering Lab, National Center for Scientific Research "Demokritos"](https://www.iit.demokritos.gr/skel) ,
* [Science-For-You not-for-profit company](http://www.scify.gr/site/en) and, 
* [LIPADE, Paris Descartes University](http://lipade.mi.parisdescartes.fr)
