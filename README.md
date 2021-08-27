# sparkQueries

This program uses scala, spark, and a hive database to run queries and find meaningful trends in the data. 

## Technologies Used
* Spark 2.3.1
* Spark-Hive 2.3.1
* Scala 2.11.2
* Java sdk 1.8

## Getting Started
This was run on Windows, using IntelliJ.
To run this yourself:
* Make sure java 8 is installed on your machine
* Install the scala plugin on IntelliJ
* Install [winutils](https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3). Install where you want, but my code assumes it's installed at `C:\\winutils`

## Usage
* Compiling and running p1query will execute all of the queries. Your output will look something like this, which finds all the shared drinks between branch 4 and 7

![image](https://user-images.githubusercontent.com/58571104/130814834-ea8ad0f8-f2bd-4596-abb2-9be19d405f9b.png)

## License
This project uses the MIT License
