package gov.darpa.hive

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import org.apache.spark.graphx.util.GraphGenerators


/*
Example use of maxFlow function
(note that maxFlow function needs to be imported in order to run this)
 */

object Driver {

def shortestPath(sourceId: VertexId, targetId: VertexId, graph: Graph[Long, Int]): (Set[(VertexId, VertexId)], Int) = {
  val test: VertexId = sourceId // default vertex id

  // Initialize the graph such that all vertices except the root have distance infinity.
  // Vertices will have attributes (distance,capacity,id)
  // Note that Int.MaxValue - 1 is used to symbolise infinity

  // initializing the vertex attributes
  val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) (0, Int.MaxValue - 1, id)
  else (Int.MaxValue - 1, Int.MaxValue - 1, id))

  val sssp = initialGraph.pregel((Int.MaxValue - 1, Int.MaxValue - 1, test))(

    // Vertex program
    (id, dist, newDist) => {
      if (dist._1 < newDist._1) dist
      else newDist
    },

    // send message
    triplet => {
      // If distance of source + 1 is less than attribute of destination => update
      // If capacity along edges <= 0; do not propagate message
      if (triplet.srcAttr._1 + 1 < triplet.dstAttr._1 && triplet.attr > 0) {
        Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, math.min(triplet.srcAttr._2, triplet.attr), triplet.srcId)))
      }
      else {
        Iterator.empty
      }
    },

    // merge multiple messages
    (a, b) => {
      if (a._1 < b._1) a // if distance from a is less than distance from b 
      else if ((a._1 == b._1) && (a._2 > b._2)) a // if they're equal but a has a higher minimum capacity seen so far
      else b
    }
  )

  val vNum = sssp.vertices.count.toInt // Number of elements < n
  val v = sssp.vertices.take(vNum) // Convert RDD to array
  val minCap = sssp.vertices.filter(v => v._1 == targetId).first._2._2
  val path = Set[(VertexId, VertexId)]()


  // Check if there is a path or not; if no path => return empty set
  if (minCap != (Int.MaxValue - 1)) {
    val links = new HashMap[VertexId, VertexId]
    for (i <- 0 to vNum - 1) {
      links += v(i)._1 -> v(i)._2._3
    }

    // Build the set of edges in the shortest path
    var id = targetId
    for (i <- 0 to vNum - 1; if id != sourceId) {
      path += ((links(id), id))
      id = links(id)
    }
  }

  return (path, minCap)
}


def maxFlow ( sc: SparkContext, sourceId: VertexId, targetId: VertexId, graph: Graph[Long,Int] ) : RDD[((VertexId,VertexId),Int)] = {

  val edges = graph.edges
  val vertices = graph.vertices
  var flows: RDD[((VertexId, VertexId), Int)] = edges.map(e => ((e.srcId, e.dstId), 0))
  var residual: Graph[Long, Int] = graph // Initially zero flow => residual = graph

  var shortest = shortestPath(sourceId, targetId, residual)
  var path = shortest._1
  var minCap = shortest._2
  val empty = Set[(VertexId, VertexId)]() // Empty set

  // Note that algorithm only terminates when there are no more shortest paths
  // This could potentially be a long time, so the code user should edit in a max number
  // of iterations here if needed
  while (path != empty) {
    val bcPath = sc.broadcast(path)

    // Update the flows
    val updateFlow = minCap
    val newFlows = flows.map(e => {
      if (bcPath.value contains e._1) {
        (e._1, e._2 + updateFlow)
      }
      else {
        e
      }
    })

    flows = newFlows

    // Update the residual graph
    val newEdges = residual.edges.map(e => ((e.srcId, e.dstId), e.attr)).
      flatMap(e => {
      if (bcPath.value contains e._1) {
        Seq((e._1, e._2 - minCap), ((e._1._2, e._1._1), minCap))
      }
      else Seq(e)
    }
      ).reduceByKey(_ + _)

    residual = Graph(vertices, newEdges.map(e => Edge(e._1._1, e._1._2, e._2)))

    // Compute for next iteration
    shortest = shortestPath(sourceId, targetId, residual)
    path = shortest._1
    minCap = shortest._2
  }

  return flows
}

def main(args: Array[String]) {
// Running maxFlow on a few random graphs
val sc: SparkContext = new SparkContext()

for (i <- 1 to 3 )
{
  var n: Int = 10*i // Number of vertices

  // Create random graph with random source and target
  var graph = GraphGenerators.logNormalGraph(sc, n, 5)
  val r = scala.util.Random
  val sourceId: VertexId = r.nextInt(n) // The source
  val targetId: VertexId = r.nextInt(n) // The target

  // Calculate Max Flow
  val t0 = System.nanoTime()
  val flows = maxFlow(sc, sourceId, targetId, graph)
  val t1 = System.nanoTime()

  // Print information
  val emanating = flows.filter(e => e._1._1 == sourceId).map(e => (e._1._1,e._2)).reduceByKey(_ + _).collect
  println("Number of Edges: ")
  println(flows.count)
  println("Max Flow: ")
  println(emanating(0)._2)
  println("Time: ")
  println((t1 - t0)/1e9)
}



/*

//To use a graph imported from a txt file, uncomment this section

// Import vertices
val vertices = sc.textFile("data/toy-vertices-big.txt").
  flatMap(line => line.split(" ")).
  map(l => (l.toLong, 0.toLong)) // Vertex needs a property

// Import Edges
val edges = sc.textFile("data/toy-edges-big.txt").
  map(line => line.split(" ")).
  map(e => ((e(0).toLong, e(1).toLong), e(2).toInt)).
  reduceByKey(_ + _).
  map(e => Edge(e._1._1, e._1._2, e._2))

// Create Graph
val g: Graph[Long,Int] = Graph(vertices, edges)

val s: VertexId = 42 // The source
val t: VertexId = 73 // The target

val t0 = System.nanoTime()
val flows = maxFlow(s, t, g)
val t1 = System.nanoTime()

println("Time: ")
println((t1 - t0)/1e9)

val emanating = flows.filter(e => e._1._1 == s).map(e => (e._1._1,e._2)).reduceByKey(_ + _).collect
println("Max Flow: ")
println(emanating(0)._2)

*/
}
}
