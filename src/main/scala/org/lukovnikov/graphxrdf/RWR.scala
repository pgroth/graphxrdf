package org.lukovnikov.graphxrdf

import org.apache.spark.graphx.{Graph}

object RWR extends RDFGraphExecutable{
	
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

		var aggv = graph
					.mapVertices((id, x) => (0, Map[Long, Double](id -> 1.0)))
					.joinVertices(graph.outDegrees){
						(id, x, y) => (y, x._2)
					}
		var agg = aggv.vertices
		var inbox = Graph[(Int,Map[Long, Double]), String](agg, graph.edges)
		var outgv = agg.map(v => (v._1, Map[Long,Double]()))
		var outg = Graph[Map[Long,Double], String](outgv, graph.edges)
		
		
		var iter = numiter
		while (iter > 0) {
			agg = inbox.aggregateMessages[(Int,Map[Long,Double])](
				triplet => {
					val distId = triplet.dstId
					val sourcedeg = triplet.srcAttr._1
					var ret = Map[Long, Double]()//Map(distId -> 1.0/sourcedeg)
					if (triplet.dstAttr != null)
						for (entry <- triplet.dstAttr._2) {
							val score = entry._2/sourcedeg
							if (score > thresh)
								ret += (entry._1 -> entry._2/sourcedeg)
						}
					triplet.sendToSrc((sourcedeg, ret))
				},
				(a,b) => {
					// merge inboxes
					val degree = a._1
					var inbox = scala.collection.mutable.Map[Long, Double]()
					for (axe <- a._2) {
						inbox(axe._1) = axe._2
					}
					for (bxe <- b._2) {
						if (inbox.contains(bxe._1)) {
							inbox(bxe._1) += bxe._2
						} else {
							inbox(bxe._1) = bxe._2
						}
					}
					(degree, Map() ++ inbox) 
				}
			)
			outg = outg.joinVertices(agg)
				{(a,b,c) => {
						val ret = scala.collection.mutable.Map() ++ b
						for (x <- c._2) {
							if (ret.contains(x._1)) {
								ret(x._1) += x._2
							}else{
								ret(x._1) = x._2
							}
						}
						Map() ++ ret.toSeq.sortWith(_._2 > _._2).take(limit).toMap
					}	
				}
			inbox = Graph(agg, inbox.edges)
			iter -= 1
		}
		val origvertices = graph.vertices.take(20)
		val outgvcollect = outg.vertices.take(20)
		val aggrvertices = inbox.vertices.take(20)
		for (vertex <- aggrvertices) {
			Console.println(vertex)
			//Console.println(vertex._1.toString + ": " + vertex._2.toString)
		}
		for (vertex <- outgvcollect) {
			Console.println(vertex)
		}
		for (vertex <- origvertices) {
			Console.println(vertex._1.toString + ": " + vertex._2)
		}
		return outg
	}

}