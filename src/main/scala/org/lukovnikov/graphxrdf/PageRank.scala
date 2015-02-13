package org.lukovnikov.graphxrdf

import org.apache.spark.graphx.{Graph}

object PageRank extends RDFGraphExecutable {
	
	def execute(graph:Graph[String, String], args:Double*):Graph[_, _] = {
		val tol:Double = if (args.length > 0) args(0).doubleValue else 10
		val resetProb:Double = if (args.length > 1) args(1).doubleValue else 0.0001
		return graph.pageRank(tol, resetProb)
	}

}