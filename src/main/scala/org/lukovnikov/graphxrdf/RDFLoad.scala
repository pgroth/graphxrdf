package graphxTest

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.graphx.Graph


object RDFLoad {
	def main(args:Array[String]) = {
		val scc = new SparkConf
		val sc = new SparkContext(scc)
		var src = "/home/denis/dev/sparkdev/graphxrdf/src/main/scala/bigsample.nt"
		var numiter = 2
		if (args.length > 0) {
			src = args(0)
		}
		if (args.length > 1) {
			numiter = args(1).toInt
		}
		Console.println(src)
		val graph = RDFLoader.loadNTriples(sc, src)
		Console.println(graph.edges.count)
		var thresh = 1000
		breakable {
			for (vertex <- graph.vertices.take(1000)) {
				Console.println(vertex)
				thresh -= 1
				if (thresh < 0) break
			}
		}
		rwr(graph,numiter)
	}
	
	def rwr(graph:Graph[String,String], numiter:Int = 2) = {
		var outdegrees = graph.outDegrees.map(
				outdeg => 
					(outdeg._1, (outdeg._2, Map[Long, Double](outdeg._1 -> 1.0)))
				)
		var inbox = Graph[(Int,Map[Long, Double]), String](outdegrees, graph.edges)
		for (vertex <- outdegrees.collect)
			Console.println(vertex)
		//g2 = g2.mapEdges(edge => null)
		var iter = numiter
		while (iter > 0) {
			val agg = inbox.aggregateMessages[(Int,Map[Long,Double])](
				triplet => {
					val distId = triplet.dstId
					val sourcedeg = triplet.srcAttr._1
					var ret = Map[Long, Double]()//Map(distId -> 1.0/sourcedeg)
					if (triplet.dstAttr != null)
						for (entry <- triplet.dstAttr._2) {
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
					// merge inbox into state
					/* var state = scala.collection.mutable.Map() ++ a._2
					for (x <- inbox) {
						if (state.contains(x._1)) {
							state(x._1) += x._2
						} else {
							state(x._1) = x._2
						}
					} // */
					(degree, Map() ++ inbox) 
				}
			)
			inbox = Graph(agg, inbox.edges)
			iter -= 1
		}
		val origvertices = graph.vertices.collect
		val aggrvertices = inbox.vertices.collect
		for (vertex <- aggrvertices) {
			Console.println(vertex)
			//Console.println(vertex._1.toString + ": " + vertex._2.toString)
		}
		for (vertex <- origvertices) {
			Console.println(vertex._1.toString + ": " + vertex._2)
		}
	}

}