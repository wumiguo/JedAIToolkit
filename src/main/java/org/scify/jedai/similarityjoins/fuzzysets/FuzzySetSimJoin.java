/*
* Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package org.scify.jedai.similarityjoins.fuzzysets;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.jgrapht.alg.matching.MaximumWeightBipartiteMatching;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import java.util.*;

public class FuzzySetSimJoin {

    long startTime, stopTime, transformationTime, indexingTime, joinTime, signatureGenerationTime = 0,
            checkFilterTime = 0, nnFilterTime = 0, verificationTime = 0;
    int totalCheckFilterCandidates = 0, totalNNFilterCandidates = 0, totalMatches = 0;
    TObjectIntMap<String> tokenDict;
    double[] elementBounds;

    /**
     * Computes the join between two collections
     */
    public HashMap<String, Double> join(Map<String, List<Set<String>>> input1, Map<String, List<Set<String>>> input2,
            double simThreshold) {
//		public List<int[]> join(Map<String, List<Set<String>>> input1, Map<String, List<Set<String>>> input2, double simThreshold) {

        /* TRANSFORM THE INPUT COLLECTIONS */
        transformationTime = System.nanoTime();
        InputTransformer it = new InputTransformer();

        // Create an empty token dictionary
        tokenDict = new TObjectIntHashMap<String>();
        // use instead the following if order by frequency is needed
        // TObjectIntMap<String> tokenDict =
        // it.mapTokensToIntsByFrequency(input2.values());

        // Transform input to integer tokens
        // (dictionary is built on input2)
        int[][][] collection2 = it.transform(input2, tokenDict);
        int[][][] collection1 = it.transform(input1, tokenDict);

        // --for debugging--
        // Util.printTokenMap(tokenDict, Math.min(20, tokenDict.size()));
        // Util.printCollection(collection1, Math.min(10, collection1.length));
        // Util.printCollection(collection2, Math.min(10, collection2.length));
        transformationTime = System.nanoTime() - transformationTime;

        /* JOIN THE TRANSFORMED INPUT COLLECTIONS */
        HashMap<String, Double> matchingPairs = join(collection1, collection2, simThreshold);

        return matchingPairs;
    }

    /**
     * Computes the join between two already transformed and indexed collections
     */
    public HashMap<String, Double> join(int[][][] collection1, int[][][] collection2, double simThreshold) {
//		public List<int[]> join(int[][][] collection1, int[][][] collection2, double simThreshold) {

//		List<int[]> matchingPairs = new ArrayList<int[]>();
        HashMap<String, Double> matchingPairs = new HashMap<String, Double>();

        /* CREATE INDEX */
        indexingTime = System.nanoTime();
        IndexConstructor ic = new IndexConstructor();
        TIntObjectMap<TIntList>[] idx = ic.buildSetInvertedIndex(collection2, tokenDict.size());
        indexingTime = System.nanoTime() - indexingTime;
        // Util.printSetInvertedIndex(idx, Math.min(20, idx.length));

        /* EXECUTE THE JOIN ALGORITHM */
        joinTime = System.nanoTime();
        int total_steps = 20;
        int step = collection1.length / total_steps;

        for (int i = 0; i < collection1.length; i++) {
            // progress bar
            if (step == 0) {
                step++;
            }
            if (i % step == 0) {
                System.out.print("|");
                for (int j = 0; j <= (i / step); j++) {
                    System.out.print("=");
                }
                for (int j = (i / step); j < total_steps; j++) {
                    System.out.print(" ");
                }
                System.out.print("|" + (i / step * 100) / total_steps + "% \r");
//				System.out.print("|"+"=".repeat(i/step)+" ".repeat(total_steps-i/step)+"|"+(i/step*100)/total_steps+"% \r");
            }

            TIntDoubleHashMap matches = search(collection1[i], collection2, simThreshold, idx);
            for (int j : matches.keys()) {
                // matchingPairs.add(new int[] { i, j });
                matchingPairs.put(i + "_" + j, matches.get(j));
            }
        }
        joinTime = System.nanoTime() - joinTime;

        return matchingPairs;
    }

    /**
     * Find matches for a given set
     */
    private TIntDoubleHashMap search(int[][] querySet, int[][][] collection, double simThreshold,
            TIntObjectMap<TIntList>[] idx) {

        /* SIGNATURE GENERATION */
        startTime = System.nanoTime();
        TIntSet[] unflattenedSignature = computeUnflattenedSignature(querySet, simThreshold, idx);
        // Util.printUnflattenedSignature(unflattenedSignature); // debugging
        signatureGenerationTime += System.nanoTime() - startTime;

        /* CANDIDATE SELECTION AND CHECK FILTER */
        startTime = System.nanoTime();
        TIntObjectMap<TIntDoubleMap> checkFilterCandidates = applyCheckFilter(querySet, collection,
                unflattenedSignature, idx, simThreshold);
        // Util.printCheckFilterCandidates(checkFilterCandidates); // debugging
        checkFilterTime += System.nanoTime() - startTime;

//		System.out.println(checkFilterCandidates.toString());

        /* NEAREST NEIGHBOR FILTER */
        startTime = System.nanoTime();
        TIntSet nnFilterCandidates = applyNNFilter(querySet, collection, checkFilterCandidates, simThreshold);
        nnFilterTime += System.nanoTime() - startTime;

        /* VERIFICATION */
        startTime = System.nanoTime();
        TIntDoubleHashMap matches = verifyCandidates(querySet, collection, nnFilterCandidates, simThreshold);
        verificationTime += System.nanoTime() - startTime;

        totalCheckFilterCandidates += checkFilterCandidates.size();
        totalNNFilterCandidates += nnFilterCandidates.size();
        totalMatches += matches.size();

        // return the surviving candidates
//		return matches.toArray();
        return matches;
    }

    private TIntSet[] computeUnflattenedSignature(int[][] querySet, double simThreshold,
            TIntObjectMap<TIntList>[] idx) {

        // initialize unflattened signature
        TIntSet[] unflattenedSignature = new TIntHashSet[querySet.length];
        for (int i = 0; i < unflattenedSignature.length; i++) {
            unflattenedSignature[i] = new TIntHashSet();
        }

        // Compute token scores
        double score;
        TIntDoubleMap tokenScores = new TIntDoubleHashMap();
        // first compute values
        for (int i = 0; i < querySet.length; i++) {
            for (int j = 0; j < querySet[i].length; j++) {
                score = 0;
                if (tokenScores.containsKey(querySet[i][j])) {
                    score = tokenScores.get(querySet[i][j]);
                }
                score += (1.0 / querySet[i].length);
                tokenScores.put(querySet[i][j], score);
            }
        }
        // then include costs
        int cost;
        for (int token : tokenScores.keys()) {
            if (token < 0) {
                tokenScores.put(token, 0);
            } else {
                cost = 0;
                for (int s : idx[token].keys()) {
                    cost += idx[token].get(s).size();
                }
                tokenScores.put(token, cost / tokenScores.get(token));
            }
        }

        // set threshold and current bound
        double thres = simThreshold * querySet.length;
        double simUpperBound = querySet.length;

        // construct the signature
        int bestToken = -1;
        double bestScore = Double.MAX_VALUE;
        while (simUpperBound >= thres) {
            // choose the next best token
            for (int token : tokenScores.keys()) {
                score = tokenScores.get(token);
                if (score < bestScore) {
                    bestToken = token;
                    bestScore = score;
                }
            }

            // remove it from the pool of tokens and reset best score
            tokenScores.remove(bestToken);
            bestScore = Double.MAX_VALUE;

            // update the signature
            for (int i = 0; i < querySet.length; i++) {
                for (int j = 0; j < querySet[i].length; j++) {
                    if (querySet[i][j] == bestToken) {
                        unflattenedSignature[i].add(bestToken);
                        simUpperBound -= (1.0 / (double) querySet[i].length);
                    }
                }
            }
        }

        return unflattenedSignature;
    }

    private TIntObjectMap<TIntDoubleMap> applyCheckFilter(int[][] querySet, int[][][] collection,
            TIntSet[] unflattenedSignature, TIntObjectMap<TIntList>[] idx, double simThreshold) {

        TIntObjectMap<TIntDoubleMap> checkFilterCandidates = new TIntObjectHashMap<TIntDoubleMap>();

        TIntDoubleMap cachedScores;
        double sim;

        // compute bounds for length filter
        double minLength = querySet.length * simThreshold;
        double maxLength = querySet.length / simThreshold;

        // compute the individual element bounds
        elementBounds = new double[querySet.length];
        for (int i = 0; i < querySet.length; i++) {
            elementBounds[i] = (double) (querySet[i].length - unflattenedSignature[i].size())
                    / (double) querySet[i].length;
        }

        // for each element of the query set
        for (int i = 0; i < querySet.length; i++) {

            // for each token, retrieve candidates from index
            for (int token : unflattenedSignature[i].toArray()) {
                if (token < 0) {
                    continue;
                }

                for (int s : idx[token].keys()) {
                    // Apply length filter
                    if (collection[s].length < minLength || collection[s].length > maxLength) {
                        continue;
                    }

                    for (int e : idx[token].get(s).toArray()) {

                        // compute the similarity score
                        sim = jaccard(querySet[i], collection[s][e]);

                        // check the condition
                        if (sim >= elementBounds[i]) {
                            cachedScores = checkFilterCandidates.get(s);
                            if (cachedScores == null) {
                                cachedScores = new TIntDoubleHashMap();
                                cachedScores.put(i, sim);
                            } else if (sim > cachedScores.get(i)) {
                                cachedScores.put(i, sim);
                            }
                            checkFilterCandidates.put(s, cachedScores);
                        }
                    }
                }
            }
        }

        return checkFilterCandidates;
    }

    private TIntSet applyNNFilter(int[][] querySet, int[][][] collection,
            TIntObjectMap<TIntDoubleMap> checkFilterCandidates, double simThreshold) {

        TIntSet nnFilterCandidates = new TIntHashSet();

        double sim, maxSim, total;
        TIntSet matchedElements;

        candLoop:
        for (int c : checkFilterCandidates.keys()) {
            total = 0;
            matchedElements = new TIntHashSet();

            for (int e : checkFilterCandidates.get(c).keys()) {
                matchedElements.add(e);
                total += checkFilterCandidates.get(c).get(e);
            }

            for (int j = 0; j < querySet.length; j++) {
                if (matchedElements.contains(j)) {
                    continue;
                }
                maxSim = 0;
                for (int k = 0; k < collection[c].length; k++) {
                    sim = jaccard(querySet[j], collection[c][k]);
                    if (sim > maxSim) {
                        maxSim = sim;
                    }
                }
                total += maxSim;
                matchedElements.add(j);
                if (total < simThreshold * matchedElements.size()) {
                    continue candLoop;
                }
            }
            if (total >= simThreshold * querySet.length) {
                nnFilterCandidates.add(c);
            }
        }

        return nnFilterCandidates;
    }

    private TIntDoubleHashMap verifyCandidates(int[][] querySet, int[][][] collection, TIntSet nnFilterCandidates,
            double simThreshold) {

        TIntSet matches = new TIntHashSet();
        TIntDoubleHashMap matches2 = new TIntDoubleHashMap();
        TIntIterator sit = nnFilterCandidates.iterator();

        while (sit.hasNext()) {
            int id_s = sit.next();

            SimpleWeightedGraph<String, DefaultWeightedEdge> g = new SimpleWeightedGraph<String, DefaultWeightedEdge>(
                    DefaultWeightedEdge.class);
            HashSet<String> r_partition = new HashSet<String>();
            HashSet<String> s_partition = new HashSet<String>();

            for (int id_r = 0; id_r < querySet.length; id_r++) {
                String r_v = "r_" + id_r;
                g.addVertex(r_v);
                r_partition.add(r_v);
                for (int id_ss = 0; id_ss < collection[id_s].length; id_ss++) {
                    String s_v = "s_" + id_ss;
                    double sim = jaccard(querySet[id_r], collection[id_s][id_ss]);
                    g.addVertex(s_v);
                    s_partition.add(s_v);
                    DefaultWeightedEdge e = g.addEdge(r_v, s_v);
                    g.setEdgeWeight(e, sim);
                }
            }

            double match = 0.0;
            //System.out.println(g.edgeSet().size()+"asd"+g.vertexSet().size());
            MaximumWeightBipartiteMatching<String, DefaultWeightedEdge> matching = new MaximumWeightBipartiteMatching<String, DefaultWeightedEdge>(
                    g, r_partition, s_partition);
            for (DefaultWeightedEdge ed : matching.getMatching().getEdges()) {
                //System.out.println(g.getEdgeWeight(ed)+" "+g.getEdgeSource(ed)+" "+g.getEdgeTarget(ed));
                match += g.getEdgeWeight(ed);
            }
            //System.out.println();

            double sim = match / (querySet.length + collection[id_s].length - match);
            if (sim >= simThreshold) {
                matches.add(id_s);
                matches2.put(id_s, sim);
            }
        }
//		System.out.println(matches.toString());
//		System.out.println(matches2.toString());
        return matches2;

    }

    private static double jaccard(int[] r, int[] s) {

        TIntSet nr = new TIntHashSet(r);
        TIntSet ns = new TIntHashSet(s);
        TIntSet intersection = new TIntHashSet(nr);
        intersection.retainAll(ns);
        TIntSet union = new TIntHashSet(nr);
        union.addAll(ns);
        return ((double) intersection.size()) / ((double) union.size());
    }
}
