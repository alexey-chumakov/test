package test

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions}

object TestDistances extends AppBase {

  import spark.implicits._

  // Test data
  private val nodes: DataFrame = createDataFrame(Seq(
    """{"node_id": 1, "lon": 1, "lat": 1}""",
    """{"node_id": 2, "lon": 2, "lat": 2}""",
    """{"node_id": 3, "lon": 3, "lat": 3}""",
    """{"node_id": 4, "lon": 4, "lat": 4}""",
    """{"node_id": 5, "lon": 5, "lat": 5}""",
    """{"node_id": 6, "lon": 6, "lat": 6}""",
    """{"node_id": 7, "lon": 7, "lat": 7}""",
    """{"node_id": 8, "lon": 8, "lat": 8}"""
  ))

  private val ways: DataFrame = createDataFrame(Seq(
    """{"way_id": "way1", "nodes": [1, 2, 3, 5]}""",
    """{"way_id": "way2", "nodes": []}""",
    """{"way_id": "way3", "nodes": [5, 6]}""",
    """{"way_id": "way4", "nodes": [1, 5]}"""
  ))

  // Add UDF for naive distance calculation (just euclidean distance for testing)
  spark.udf.register("distance", functions.udf(distance(_, _, _, _)))

  // Explode and join (empty ways will be omitted)
  val waysExploded = ways.select($"way_id", functions.posexplode($"nodes").as(Seq("pos", "node_id")))
  val waysJoined = waysExploded.join(nodes, "node_id")

  // Create a window over way_id and calculate distances between nodes
  val windowSpec = Window.partitionBy($"way_id").orderBy($"pos")
  val distances = waysJoined
    .withColumn("prev_lat", functions.lag("lat", 1).over(windowSpec))
    .withColumn("prev_lon", functions.lag("lon", 1).over(windowSpec))
    .filter("prev_lon is not null AND prev_lat is not null")
    .withColumn("distance", functions.expr("distance(lat, lon, prev_lat, prev_lon)"))
    .groupBy("way_id").sum("distance")

  distances.show()

}