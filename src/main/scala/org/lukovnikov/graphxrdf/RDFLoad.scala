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
		var out = "/home/denis/dev/sparkdev/graphxrdf/src/main/scala/sample.rwr.out"
		var dictout = "/home/denis/dev/sparkdev/graphxrdf/src/main/scala/sample.dict.out"
		var numiter = 2
		var limit = 100
		var threshold = 0.001
		if (args.length > 0) {
			src = args(0)
		}
		if (args.length > 1) {
			out = args(1)
		}
		if (args.length > 2) {
			dictout = args(2)
		}
		if (args.length > 3) {
			numiter = args(3).toInt
		}
		if (args.length > 4) {
			limit = args(4).toInt
		}
		if (args.length > 5)
			threshold = args(5).toDouble
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
		RDFLoader.getdictionary(graph).saveAsTextFile(dictout)
		rwr(graph,numiter,limit,threshold).vertices.saveAsTextFile(out)
	}
	
	def rwr(
			graph:Graph[String,String], 
			numiter:Int = 2, 
			limit:Int = 100, 
			thresh:Double = 0.001)
		:Graph[Map[Long,Double],String] = {
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
