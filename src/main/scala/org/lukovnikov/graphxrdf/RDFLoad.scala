package graphxTest

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.Breaks._


object RDFLoad {
	def main(args:Array[String]) = {
		val scc = new SparkConf
		val sc = new SparkContext(scc)
		var src = "/home/denis/dev/sparkdev/graphxrdf/src/main/scala/bigsample.nt"
		if (args.length > 0) {
			src = args(0)
		}
		Console.println(src)
		val graph = RDFLoader.loadNTriples(sc, src)
		Console.println(graph.edges.count)
		var thresh = 1000
		breakable {
			for (vertex <- graph.vertices.collect) {
				Console.println(vertex)
				thresh -= 1
				if (thresh < 0) break
			}
		}
	}

}