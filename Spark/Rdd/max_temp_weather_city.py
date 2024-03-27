from pyspark import SparkContext

sc = SparkContext(master="local", appName="MaxCityTemp")

weather_rdd = sc.textFile("./inputData/weather.csv")
zips_city_rdd = sc.textFile("./inputData/zips_city.csv")

def get_zip_code_temp(line):
    _, zip_code,temperature = line.split(",")
    return (int(zip_code), int(temperature))


def get_zip_city(line):
    zip_code, city = line.split(",")
    return (int(zip_code), city)


zips_temp_rdd = weather_rdd.map(lambda line: get_zip_code_temp(line))
max_temp_per_zip_rdd = zips_temp_rdd.reduceByKey(lambda x,y: max(x,y) )

zips_city = zips_city_rdd.map(lambda line: get_zip_city(line))

zip_code_city_join = max_temp_per_zip_rdd.join(zips_city)

print("City with Max Temp", zip_code_city_join.map(lambda item: (item[1][1], item[1][0])).collect())
