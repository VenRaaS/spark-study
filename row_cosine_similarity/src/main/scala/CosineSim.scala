package org.venraas.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix, IndexedRow, IndexedRowMatrix}
import collection.JavaConversions._
import scopt.OptionParser


object CosineSim {

    case class Params (
        input: String = null,
        output: String = null
    ) extends AbstractParams[Params]


    def main(args: Array[String]) {
	val defaultParams = Params()
	val parser = new OptionParser[Params]("Cosine Similarity") {
	    head("CosineSimilarity: Compute the similar rows of a matrix, using cosine similarity")

	    arg[String]("<input>")
		.text("input paths to input data set, whose file format is that each line " +
		      "contains an user feature item forms as <uid>,<category_code>,<value>")
		.required()
		.action((x, c) => c.copy(input = x))

	    arg[String]("<output>")
		.text("output paths to output data set, whose file format is that each line " +
		      "contains ...  which forms as ...<Antecedent>Tab<Consequent>Tab<Confidence>")
		.required()
		.action((x, c) => c.copy(output = x))
	}

	parser.parse(args, defaultParams)
	.map { params =>
	    println("input: %s".format(params.input) )
	    println("output: %s".format(params.output) )
	    run(params)
	}
	.getOrElse (
	    sys.exit(1)
	)
    }

    def run(params: Params) {
        val conf = new SparkConf().setAppName("Cosine similarity for rows")

        val sc = new SparkContext(conf)

	//-- input data with form as
        //   ($uid, $category_code, $value)
//        val txt = sc.textFile("hdfs://itrihd34:8020/tmp/user_p3category_preferences/*")
        val txt = sc.textFile(params.input)

        //-- ($category_code, ($uid, $value))
	val inData = txt.map(l => { 
		var ary = l.trim.split(',');
		(ary(1), (ary(0), ary(2)));
	})

	//-- mapping table of category_code 2 index 
        //   i.e. ($category_code, $index)
        var cat2idx = inData.map(t => t._1).distinct().zipWithIndex()
//	cat2idx.take(100).foreach { println; }
	
	//-- re-mapping feature index and forms as 
	//   ($uid, ($feaIdx, $faaVal))
	var u2feaIdxVal = inData.join(cat2idx).map( t => (t._2._1._1, (t._2._2.toInt, t._2._1._2.toDouble)) )
//	u2feaIdxVal.take(1000).foreach{ println; }

	//-- user vector with category_codes, 
	//   i.e. ($uid, [($feaIdx, $faaVal)] )
	var num_cats = cat2idx.collect().length
	var u2vct = u2feaIdxVal.groupByKey().map(t => { (t._1, Vectors.sparse(num_cats, t._2.toSeq)) })
//	u2vct.take(1000).foreach{ println; }

	//-- mapping table of uid 2 index
	val u2idx = txt.map(l => { l.trim.split(',')(0) }).distinct().zipWithIndex()
//	u2idx.take(100).foreach{ println; }
	val rows = u2idx.join(u2vct).map( t => new IndexedRow(t._2._1.toLong, t._2._2) )
//	rows.take(100).foreach{ println; }

	val mat:IndexedRowMatrix = new IndexedRowMatrix(rows)
//	println(mat.numCols())
//	println(mat.numRows())

	//-- calc Cosine Similarities
	val exact = mat.toCoordinateMatrix().transpose().toRowMatrix().columnSimilarities()
	exact.toRowMatrix().rows.take(100).foreach{ println; }


//    trans
//    .map(
//        t => t.mkString("\t")
//    )
//    .saveAsTextFile(
//        "hdfs://itrihd34:8020/tmp/tmp_gohappy_ar_trans"
//    )


       println("done")
       sc.stop()
    }
}

