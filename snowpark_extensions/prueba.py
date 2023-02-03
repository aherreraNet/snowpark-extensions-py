import findspark
import re
findspark.find()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()

from pyspark.sql.functions import split
df = spark.createDataFrame([('miAylamiBsonmis',)], ['s',])

res = df.select(split(df.s, 'mi(A|BB)', 0).alias('s')).show()


target_string = 'pruebaAylapruebaBsondosBBpruebas'
pattern = 'prueba(A|BB)'

print(re.findall(pattern, target_string)) 
print(re.split(pattern, target_string, re.VERBOSE))

 
create function add(x integer, y integer)returns integerlanguage javahandler='TestAddFunc.add'target_path='@~/TestAddFunc.jar'as$$
class TestAddFunc {        public static int add(int x, int y) {          return x + y;        }}$$;