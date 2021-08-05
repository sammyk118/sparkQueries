import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object p1Query {

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    spark.sparkContext.setLogLevel("WARN") //reduces terminal clutter
    println("created spark session")

    load()
    scene1()
    scene2()
    scene3() //also calls scene 4
//      scene5()
    scene6()

    println("sql commands finished")
  }

  def scene1(): Unit ={
//    What is the total number of consumers for Branch1?
//    What is the number of consumers for the Branch2?
    println("==========scene 1 start")

    spark.sql("drop view if exists branch1")

    spark.sql("create view if not exists branch1 as select * from branch where branch.branch='Branch1'")
//    spark.sql("select * from branch1").show
    spark.sql("select sum(conscount) as branch1Sum from cons, branch1 where branch1.product = cons.drink").show

    spark.sql("create view if not exists branch2 as select * from branch where branch.branch='Branch2'")
    spark.sql("select sum(conscount) as branch2Sum from cons, branch2 where branch2.product = cons.drink").show
    println("end of scenario 1")
  }

  def scene2(): Unit ={
//    What is the most consumed beverage on Branch1
//    What is the least consumed beverage on Branch2
//    What is the Average consumed beverage of  Branch2
    println("==========scene 2 start")

    spark.sql("create view if not exists branch1 as select * from branch where branch.branch = 'Branch1'")
    spark.sql("select drink as maxDrink, conscount from consNeat, branch1 where branch1.product = consNeat.drink order by conscount desc limit 1").show

    spark.sql("create view if not exists branch2 as select * from branch where branch.branch = 'Branch2'")
    spark.sql("select drink as minDrink, conscount from consNeat, branch2 where branch2.product = consNeat.drink order by conscount asc limit 1").show

    spark.sql("select avg(conscount) as branch2Avg from consNeat, branch2 where branch2.product = consNeat.drink").show
    println("end of scenario 2")
  }

  def scene3(): Unit ={
//    What are the beverages available on Branch7, Branch8, and Branch1?
//    what are the common beverages available in Branch4,Branch7?
    println("==========scene 3 start")

    spark.sql("select product, branch from branch where " +
      "branch.branch = 'Branch8' or branch.branch = 'Branch1' or branch.branch = 'Branch7'" +
      " order by product ").show

    spark.sql("create view if not exists branch4 as " +
      "select * from branch where branch.branch = 'Branch4'")
    spark.sql("create view if not exists branch7 as " +
      "select * from branch where branch.branch = 'Branch7'")

    spark.sql("create view if not exists branch47 as " +
      "select branch4.product, branch4.branch as branch4, branch7.branch as branch7 " +
      "from branch4 full join branch7 on branch4.product = branch7.product")
    spark.sql("select * from branch47").show
    println("Scenario 3 end")
    scene4()
  }

  def scene4(): Unit ={
//    create a partition,View for scenario3
    println("==========scene 4 start")

    spark.sql("select * from branch47").show
    spark.sql("select product, count(product)" +
      "over(partition by product) as productAmount " +
      "from branch47 order by productAmount desc, product").show
  }

  def scene5(): Unit ={
    println("==========scene 5 start")

    // Alter the table properties to add "note","comment"
    spark.sql("alter table branch ADD columns(note STRING) --this is a comment")
//    spark.sql("update branch set letternote='this is a note'")
    spark.sql("describe branch").show
  }

  def scene6(): Unit ={
    println("==========scene 6 start")
    // Remove a row from any Scenario.
    // find min, filter to everything but
    spark.sql("select * from cons order by conscount asc limit 1").show
    spark.sql("select * from cons where conscount <> " +
      "(select conscount from cons order by conscount asc limit 1)" +
      " order by conscount asc").show
    println("============== scene 6 end")
  }

  def scene7(){
  //   interesting finding: foreign keys would be helpful for this data set, relating conscount to branches.
  //   Otherwise, we get duplicate findings if we search for a drink in multiple branches

    // If we don't need foreign keys, then the conscount table should at least be tidied up to combine like entries.
    // See my consNeat table
  }

  def load(): Unit ={
    spark.sql("drop table branch")
    spark.sql("create table if not exists branch (product STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("create table if not exists cons(drink STRING, conscount INT) row format delimited fields terminated by ',' stored as textfile")

    spark.sql("truncate table cons")
    spark.sql("load data local inpath 'input/Bev_BranchA.txt' into table branch")
    spark.sql("load data local inpath 'input/Bev_BranchB.txt' into table branch")
    spark.sql("load data local inpath 'input/Bev_BranchC.txt' into table branch")

    spark.sql("load data local inpath 'input/Bev_ConsCountA.txt' into table cons")
    spark.sql("load data local inpath 'input/Bev_ConsCountB.txt' into table cons")
    spark.sql("load data local inpath 'input/Bev_ConsCountC.txt' into table cons")
    spark.sql("drop table if exists consNeat")
    spark.sql("create table if not exists consNeat(drink STRING, conscount INT) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("truncate table consNeat")
    spark.sql("insert into consNeat select drink, sum(conscount) from cons group by drink")

//    spark.sql("select * from branch order by product, branch").show //find if duplicates exist

    println("load function finished")
  }
}