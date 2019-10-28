# Databricks notebook source
# MAGIC %md
# MAGIC # Visualization Examples in Databricks
# MAGIC ![dd](https://pushpullfork.com/content/images/ad_tech_similarity.png)

# COMMAND ----------

# MAGIC %md ## <div style="float:right"><a href="$./Viz-02-Matplotlib-GGPlot">MatplotLib and GgPlot Viz</a> <b style="font-size: 160%; color: #1CA0C2;">&#8680;</b></div>

# COMMAND ----------

# MAGIC %md # Built-In Visualizations

# COMMAND ----------

# MAGIC %sh pip install 

# COMMAND ----------

# MAGIC %sql USE EURO_SOCCER_DB

# COMMAND ----------

# MAGIC %sql 
# MAGIC     SELECT * FROM GAME_EVENTS 
# MAGIC     WHERE is_goal = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT CASE WHEN shot_place_str IS NULL THEN 'Unknown' ELSE shot_place_str END shot_place, COUNT(1) AS TOT_GOALS
# MAGIC     FROM GAME_EVENTS
# MAGIC     WHERE is_goal = 1
# MAGIC    GROUP BY shot_place_str

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT country_code, COUNT(1) AS TOT_GOALS
# MAGIC     FROM GAME_EVENTS
# MAGIC     WHERE is_goal = 1
# MAGIC    GROUP BY country_code

# COMMAND ----------

# MAGIC %sql
# MAGIC       SELECT SHOT_PLACE_STR, LOCATION_STR, TOT_GOALS
# MAGIC         FROM
# MAGIC         (
# MAGIC           SELECT SHOT_PLACE_STR, LOCATION_STR, TOT_GOALS,
# MAGIC                 RANK() OVER (PARTITION BY SHOT_PLACE_STR ORDER BY TOT_GOALS DESC) goals_rank
# MAGIC            FROM
# MAGIC             (
# MAGIC               SELECT CASE WHEN LOCATION_STR IS NULL THEN 'Unknown' ELSE LOCATION_STR END LOCATION_STR, 
# MAGIC                     CASE WHEN SHOT_PLACE_STR IS NULL THEN 'Unknown' ELSE SHOT_PLACE_STR END SHOT_PLACE_STR, 
# MAGIC                     COUNT(1) AS TOT_GOALS
# MAGIC               FROM GAME_EVENTS
# MAGIC               WHERE is_goal = 1
# MAGIC               AND COUNTRY_CODE = 'ESP'
# MAGIC              GROUP BY SHOT_PLACE_STR, LOCATION_STR
# MAGIC             ) tmp_in
# MAGIC           WHERE TOT_GOALS IS NOT NULL AND TOT_GOALS <> 0
# MAGIC         ) tmp_out
# MAGIC       WHERE goals_rank <= 3
# MAGIC       AND LOCATION_STR != 'Unknown' 
# MAGIC       AND SHOT_PLACE_STR != 'Unknown'
# MAGIC       ORDER BY SHOT_PLACE_STR

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT COUNTRY_CODE, TIME_BIN, COUNT(1) TOT_GOALS
# MAGIC    FROM GAME_EVENTS
# MAGIC    WHERE is_goal = 1
# MAGIC    GROUP BY COUNTRY_CODE, TIME_BIN
# MAGIC    ORDER BY COUNTRY_CODE, TIME_BIN

# COMMAND ----------

displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
</svg>""")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Change these colors to your favorites to change the D3 visualization.
# MAGIC val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
# MAGIC val colors = colorsRDD.collect()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC displayHTML(s"""
# MAGIC <!DOCTYPE html>
# MAGIC <meta charset="utf-8">
# MAGIC <style>
# MAGIC 
# MAGIC path {
# MAGIC   fill: yellow;
# MAGIC   stroke: #000;
# MAGIC }
# MAGIC 
# MAGIC circle {
# MAGIC   fill: #fff;
# MAGIC   stroke: #000;
# MAGIC   pointer-events: none;
# MAGIC }
# MAGIC 
# MAGIC .PiYG .q0-9{fill:rgb${colors(0)}}
# MAGIC .PiYG .q1-9{fill:rgb${colors(1)}}
# MAGIC .PiYG .q2-9{fill:rgb${colors(2)}}
# MAGIC .PiYG .q3-9{fill:rgb${colors(3)}}
# MAGIC .PiYG .q4-9{fill:rgb${colors(4)}}
# MAGIC .PiYG .q5-9{fill:rgb${colors(5)}}
# MAGIC .PiYG .q6-9{fill:rgb${colors(6)}}
# MAGIC .PiYG .q7-9{fill:rgb${colors(7)}}
# MAGIC .PiYG .q8-9{fill:rgb${colors(8)}}
# MAGIC 
# MAGIC </style>
# MAGIC <body>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
# MAGIC <script>
# MAGIC 
# MAGIC var width = 960,
# MAGIC     height = 500;
# MAGIC 
# MAGIC var vertices = d3.range(100).map(function(d) {
# MAGIC   return [Math.random() * width, Math.random() * height];
# MAGIC });
# MAGIC 
# MAGIC var svg = d3.select("body").append("svg")
# MAGIC     .attr("width", width)
# MAGIC     .attr("height", height)
# MAGIC     .attr("class", "PiYG")
# MAGIC     .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });
# MAGIC 
# MAGIC var path = svg.append("g").selectAll("path");
# MAGIC 
# MAGIC svg.selectAll("circle")
# MAGIC     .data(vertices.slice(1))
# MAGIC   .enter().append("circle")
# MAGIC     .attr("transform", function(d) { return "translate(" + d + ")"; })
# MAGIC     .attr("r", 2);
# MAGIC 
# MAGIC redraw();
# MAGIC 
# MAGIC function redraw() {
# MAGIC   path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
# MAGIC   path.exit().remove();
# MAGIC   path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
# MAGIC }
# MAGIC 
# MAGIC </script>
# MAGIC   """)

# COMMAND ----------

# MAGIC %md ## <div style="float:right"><a href="$./Viz-02-Matplotlib-GGPlot">MatplotLib and GgPlot Viz</a> <b style="font-size: 160%; color: #1CA0C2;">&#8680;</b></div>