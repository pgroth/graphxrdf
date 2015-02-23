package org.lukovnikov.graphxrdf

import org.apache.spark.graphx.{Graph}
import org.apache.spark.graphx.Edge
import org.apache.spark.SparkContext._

object RWX extends RDFGraphExecutable {
	
		def execute(
			graph:Graph[String,String], 
			args:Double*)
/*
			numiter:Int = 2, 
			limit:Int = 100, 
			thresh:Double = 0.001)			*/
		:Graph[Map[Long,Double],String] = {
			
		val numiter:Int = if (args.length > 0) args(0).intValue else 2
		val limit:Int = if (args.length > 1) args(1).intValue else 100
		val thresh:Double = if (args.length > 2) args(2).doubleValue else 0.001

		var sourceg = Graph(graph.outDegrees, graph.edges)
					.mapTriplets(triplet => {
						1.0 / triplet.srcAttr
					})
		
		var currg = sourceg.mapEdges(edge => (1, edge.attr))
		var iter = 0
		val dstedges = currg.edges.filter(edge => edge.attr._1 == 1)
				.map(edge => (edge.dstId.toLong, (edge.srcId.toLong, edge.attr._1, edge.attr._2)) )
		while (iter < numiter) {
			var srcedges = currg.edges.filter(edge => edge.attr._1 == iter+1)
					.map(edge => (edge.srcId.toLong, (edge.dstId.toLong, edge.attr._1, edge.attr._2)) )
			// var x = new Edge(1,2,(1,1.0))
			var newedges = srcedges.join(dstedges)
					.map(edge => new Edge(edge._2._2._1, edge._2._1._1, 
							(edge._2._1._2 + edge._2._2._2, edge._2._1._3*edge._2._2._3)))
					.filter(edge => edge.attr._2 > thresh)
					
			currg = Graph(currg.vertices, currg.edges.union(newedges))
			iter += 1
		}
		var curgv = currg.aggregateMessages[Map[Long, Double]](
				triplet => {
					Map(triplet.dstId -> triplet.attr._2)
				},
				(a,b) => {
					var inbox = scala.collection.mutable.Map[Long, Double]()
					for (axe <- a) {
						inbox(axe._1) = axe._2
					}
					for (bxe <- b) {
						if (inbox.contains(bxe._1)) {
							inbox(bxe._1) += bxe._2
						} else {
							inbox(bxe._1) = bxe._2
						}
					}
					Map() ++ inbox
				})
		return Graph(curgv, graph.edges)
	}

}