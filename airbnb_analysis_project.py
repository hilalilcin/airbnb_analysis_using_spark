from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import seaborn as sns 
#Creating Spark Session
spark = SparkSession.builder.appName("NYC Airbnb Analysis").getOrCreate()

file_path = "AB_NYC_2019.csv"
airbnb_df = spark.read.csv(file_path,header = True,inferSchema = True)
print(airbnb_df.show(5))

print(airbnb_df.printSchema())

# Checking Missing Vaalues
print("Total Number of Rows in Dataset:",airbnb_df.count())
print("Number of Rows Containing Missing Values:",airbnb_df.dropna().count())

#Filtering out Missing Values
cleaned_airbnb_df = airbnb_df.dropna()
#print(cleaned_airbnb_df.show(5))

# Converting columns to numeric types
cleaned_airbnb_df = cleaned_airbnb_df.withColumn("minimum_nights",col("minimum_nights").cast("float"))
cleaned_airbnb_df = cleaned_airbnb_df.withColumn("number_of_reviews",col("number_of_reviews").cast("float"))
cleaned_airbnb_df = cleaned_airbnb_df.withColumn("reviews_per_month",col("reviews_per_month").cast("float"))
cleaned_airbnb_df = cleaned_airbnb_df.withColumn("calculated_host_listings_count",col("calculated_host_listings_count").cast("float"))
cleaned_airbnb_df = cleaned_airbnb_df.withColumn("price",col("price").cast("float"))

#Filtering Out Rows with null values in specific columns
cleaned_airbnb_df = cleaned_airbnb_df.dropna(subset=["minimum_nights","number_of_reviews","reviews_per_month","calculated_host_listings_count","price"])
print(cleaned_airbnb_df.show(5))

# Creating a temporary table using Spark SQL
print(type(cleaned_airbnb_df))
cleaned_airbnb_df.createOrReplaceTempView("airbnb") # df is converted to use it  in SQL
# Analyzing Distribution of Prices 
price_distribution_sql = spark.sql("""
SELECT price 
FROM airbnb 
WHERE price IS NOT NULL                                                                      
""")
price_distribution_pd = price_distribution_sql.toPandas()
# Visualization
plt.figure(figsize=(10,6))
sns.histplot(price_distribution_pd['price'], bins=30, kde=False, color='skyblue')
plt.title("Price Distribution")
plt.xlabel("Price")
plt.ylabel("Frequency")
plt.grid(True)
plt.show()

# Analysing of staying time vs price
staying_time_price_sql = spark.sql("""
        SELECT minimum_nights, price
        FROM airbnb
        WHERE price IS NOT NULL AND minimum_nights IS NOT NULL
        """)
        
        
staying_time_price_pd = staying_time_price_sql.toPandas()
                                           
# VISUALIZATION
plt.figure(figsize=(10,6))
sns.scatterplot(x='minimum_nights', y='price', data=staying_time_price_pd, color='salmon')
plt.title("Minimum Nights vs. Price")
plt.xlabel("Minimum Nights")
plt.ylabel("Price")
plt.grid(True)
plt.show()

# Price Distrubiton by room type
price_room_type_sql = spark.sql("""
        SELECT room_type,price
        FROM airbnb
        WHERE price IS NOT NULL AND room_type IS NOT NULL
        """)

price_room_type_pd = price_room_type_sql.toPandas()
# VISUALIZATION
plt.figure(figsize=(10,6))
sns.boxplot(x='room_type', y='price', data=price_room_type_pd, palette='Set2')
plt.title("Price Distrubution by Room Type")
plt.xlabel("Room Type")
plt.ylabel("Price")
plt.grid(True)
plt.show()
        
# Average Price by neighbourhood
avg_price_sql = spark.sql("""
        SELECT neighbourhood, AVG(price) AS avg_price
        FROM airbnb 
        GROUP BY neighbourhood
        ORDER BY avg_price DESC
        lIMIT 10 
        """)
                           
avg_price_pd =avg_price_sql.toPandas()

# VISUALIZATION
plt.figure(figsize=(15,10))
sns.barplot(x='neighbourhood', y='avg_price', data=avg_price_pd, palette='coolwarm')
plt.title("Average Prices by Neighbourhood")
plt.xlabel("Neigbourhood")
plt.ylabel("Average Price")
plt.grid(True)
plt.show()

# Room types and staying time 
room_type_min_nights_sql = spark.sql("""
            SELECT room_type,AVG(minimum_nights) AS avg_min_nights
            FROM airbnb
            GROUP BY room_type
            ORDER BY avg_min_nights DESC
            """)
  
room_type_min_nights_pd = room_type_min_nights_sql.toPandas() 
                                  
# VISUALIZATION
plt.figure(figsize=(15,10))
sns.barplot(x='room_type', y='avg_min_nights', data=room_type_min_nights_pd, palette='viridis')
plt.title("Average Staying Time by Room Type")
plt.xlabel("Room Type")
plt.ylabel("Average Staying Time (days)")
plt.grid(True)
plt.show()  

# Price Density by Neigbourhood 
neighbourhood_price_density_sql = spark.sql("""
                            SELECT neighbourhood,price
                            FROM airbnb 
                            WHERE price > 0
                            """)
neighbourhood_price_density_pd = neighbourhood_price_density_sql.toPandas()                                           

# Visualization
plt.figure(figsize=(15,10))
sns.kdeplot(data=neighbourhood_price_density_pd,x ="price" ,hue ="neighbourhood",fill = True,palette='muted', common_norm =False,alpha = 0.3 )
plt.title("Price Density by Neigbourhood")
plt.xlabel("Price")
plt.ylabel("Density")
plt.xlim(0,500)
plt.grid(True)
plt.show() 

#Price Analysis per hosts
host_price_count_sql = spark.sql("""
            SELECT calculated_host_listings_count,AVG(price) AS avg_price
            FROM airbnb
            GROUP BY calculated_host_listings_count
            ORDER BY calculated_host_listings_count DESC
            """)     

host_price_count_pd = host_price_count_sql.toPandas()

# Visualization
plt.figure(figsize=(12, 6))
sns.lineplot(x='calculated_host_listings_count', y='avg_price', data=host_price_count_pd, marker='o', color='b')
plt.title("Average Price by Number of Host Listings")
plt.xlabel("Number of Listings by Host")
plt.ylabel("Average Price")
plt.grid(True)
plt.show()                    