package org.lukovnikov.graphxrdf

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer


trait RDFGraphExecutable {
	def main(args:Array[String]) = {
		val scc = new SparkConf
		val sc = new SparkContext(scc)
		
		var src = "/home/denis/dev/sparkdev/graphxrdf/src/main/scala/bigsample.nt"
		var out = "/home/denis/dev/sparkdev/graphxrdf/src/main/scala/sample.rwr.out"
		var filterregex = "http://dbpedia\\.org/.+"
		filterregex = null
		
		
		if (args.length > 0)
			src = args(0)
		if (args.length > 1)
			out = args(1)
		if (args.length > 2)
			filterregex = args(2)
			
		var lessargs = args.drop(2).map(x => x.toDouble)
			
		var graph = RDFLoader.loadNTriples(sc, src)
		
		if (filterregex != null && filterregex != "" && filterregex != " ")
			filterGraph(filterregex.r, graph)
		
		Console.println(graph.edges.count)
		var thresh = 1000
		breakable {
			for (vertex <- graph.vertices.take(1000)) {
				Console.println(vertex)
				thresh -= 1
				if (thresh < 0) break
			}
		}
		execute(graph, lessargs:_*).vertices.saveAsTextFile(out)
	}
	
	def execute(graph:Graph[String,String], args:Double*):Graph[_,_]
  
    def filterGraph(filterregex: scala.util.matching.Regex, graph: org.apache.spark.graphx.Graph[String,String]): Unit = {
	  graph.subgraph(x=>true,
	  	  (id, value) => value match {
	  	      case filterregex() => true
	  	      case _ => false
	  	  }
	  	)
	}

}
