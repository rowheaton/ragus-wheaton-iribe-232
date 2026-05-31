# Group project for DSC 232R: Big Data Analytics Using Spark
### Members: Tom Ragus, Roxanne Wheaton, Sergio Iribe

👉 [MusicBrainz official organization site](https://musicbrainz.org/doc/MusicBrainz_Database)

## Introduction to our project 🎶🧠

For our project, we chose to explore the expansive “MusicBrainz” dataset, a public, open-source encyclopedia of music information with over 60GB of native SQL data in Postgres with well-defined schema. The tables include data for artists, genres, instruments, labels, mediums, releases, recordings, etc., as well as the many relationships between them.

We chose this particular dataset for a couple reasons. First, it is large and complex enough to justify the use of Spark and SDSC resources. By the preprocessing stage, we were dealing with a training set with over a million rows, with each row containing a 1119-dimensional feature vector! Without Spark/ parallelization and the generous compute offered by SDSC, we would have been severely bottlenecked - creating a training set like this with local compute and regular Pandas/SQL would have been totally infeasible. Second, we were interested in the idea of building a music recommendation system. Our final XGBoost model lays the groundwork for such a system, being able to predict 1 out of 19 possible genres for a release a majority of the time based on release metadata.

Why is this important? Good recommender systems have the potential to greatly enrich our lives - bad ones can turn people into machines! We think this project ended up being quite interesting, between navigating the naturally very messy MusicBrainz dataset, to squeezing it into something workable, to then building our different models.


## Our Notebooks 📓

[Milestone 2 Notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone2/musicbrainz_v2.ipynb)

[Milestone 3 Notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone3/musicbrainz_v3.ipynb)

[Milestone 4 Notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone4/musicbrainz_v4.ipynb)


## Methods ⚙️

### Data Exploration ([notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone4/musicbrainz_v4%20(Data%20Exploration).ipynb))
We began by installing the appropriate data dumps into our environment - “mbdump” and “mbdump-derived”. Next, we explicitly defined the schema for each table we were loading in. We ingested a handful of tables including “artist”, “instrument”, “label”, “genre”, “area”, “tag”, “gender”, and “release”, and then fed the raw data into the predefined schema to create the Spark dataframes we would use for the pipeline.

```
# define “artist” table schema
artist_schema = StructType([
   StructField("id",               IntegerType(),   True),
   StructField("gid",              StringType(),    True),
   StructField("name",             StringType(),    True),
   StructField("sort_name",        StringType(),    True),
   StructField("begin_date_year",  IntegerType(),   True),
   StructField("begin_date_month", IntegerType(),   True),
   StructField("begin_date_day",   IntegerType(),   True),
   StructField("end_date_year",    IntegerType(),   True),
   StructField("end_date_month",   IntegerType(),   True),
   StructField("end_date_day",     IntegerType(),   True),
   StructField("type",             IntegerType(),   True),
   StructField("area",             IntegerType(),   True),
   StructField("gender",           IntegerType(),   True),
   StructField("comment",          StringType(),    True),
   StructField("edits_pending",    IntegerType(),   True),
   StructField("last_updated",     StringType(),    True),
   StructField("ended",            StringType(),    True),
   StructField("begin_area",       IntegerType(),   True),
   StructField("end_area",         IntegerType(),   True),
])

# define “genre” table schema
genre_schema = StructType([
   StructField("id",            IntegerType(), True),
   StructField("gid",           StringType(),  True),
   StructField("name",          StringType(),  True),
   StructField("comment",       StringType(),  True),
   StructField("edits_pending", IntegerType(), True),
   StructField("last_updated",  StringType(),  True),
])

# feed data into schemas to assemble tables
dfs = {}

for table_name, schema in schemas.items():
   print(f"\n==============================")
   print(f"Loading table: {table_name}")
   print(f"==============================")

   df = (
       spark.read
       .option("sep", "\t")
       .option("nullValue", r"\N")
       .option("header", "false")
       .option("quote", "")
       .option("escape", "")
       .schema(schema)
       .csv(f"{MBDUMP}/{table_name}")
   )

   dfs[table_name] = df
  
   row_count = df.count()
   print(f"Total {table_name} rows: {row_count}")
   duplicate_count = row_count - df.dropDuplicates().count()
   print(f"Duplicate rows in {table_name}: {duplicate_count}")

   print("=== SCHEMA ===")
   df.printSchema()

   print("=== PEEK ===")
   peek(df)
```

*Printed schema:*

<img width="621" height="281" alt="Screenshot 2026-05-31 at 1 05 35 PM" src="https://github.com/user-attachments/assets/82ee5921-48e6-4119-8e09-cd29009fb1cf" />

<img width="261" height="225" alt="Screenshot 2026-05-31 at 1 06 20 PM" src="https://github.com/user-attachments/assets/e8cbe08c-66df-441c-8734-11127c9409ce" />

Since the data was naturally very messy with schema defined by hand, we spent time manually defining a foreign key map for us to reference when performing joins later on in the data exploration and preprocessing stages.

```
# define foreign key map
primary_key_cols = {"id"}
global_id_cols = {"gid"}
foreign_key_map = {
    "artist": {
        "type":       ("artist_type", "id"),
        "area":       ("area", "id"),
        "gender":     ("gender", "id"),
        "begin_area": ("area", "id"),
        "end_area":   ("area", "id"),
    },
    "instrument": {
        "type": ("instrument_type", "id"),
    },
    "label": {
        "type": ("label_type", "id"),
        "area": ("area", "id"),
    },
    "release_group": {
        "type":          ("release_group_primary_type", "id"),
        "artist_credit": ("artist_credit", "id"),
    },
    "release": {
        "artist_credit": ("artist_credit", "id"),
        "release_group": ("release_group", "id"),
        "status":        ("release_status", "id"),
        "packaging":     ("release_packaging", "id"),
        "language":      ("language", "id"),
        "script":        ("script", "id"),
    },
    "release_label": {
        "release": ("release", "id"),
        "label":   ("label", "id"),
    },
    "artist_credit_name": {
        "artist_credit": ("artist_credit", "id"),
        "artist":        ("artist", "id"),
    },
    "label_tag": {
        "label": ("label", "id"),
        "tag":   ("tag", "id"),
    },
    "artist_tag": {
        "artist": ("artist", "id"),
        "tag":    ("tag", "id"),
    },
    "release_group_tag": {
        "release_group": ("release_group", "id"),
        "tag":           ("tag", "id"),
    },
    "l_artist_genre": {
        "link":    ("link", "id"),
        "entity0": ("artist", "id"),
        "entity1": ("genre", "id"),
    },
    "l_release_group_genre": {
        "link":    ("link", "id"),
        "entity0": ("release_group", "id"),
        "entity1": ("genre", "id"),
    },
    "l_artist_label": {
        "link":    ("link", "id"),
        "entity0": ("artist", "id"),
        "entity1": ("label", "id"),
    },
    "l_artist_release_group": {
        "link":    ("link", "id"),
        "entity0": ("artist", "id"),
        "entity1": ("release_group", "id"),
    },
    "l_artist_artist": {
        "link":    ("link", "id"),
        "entity0": ("artist", "id"),
        "entity1": ("artist", "id"),
    },
    "l_artist_instrument": {
        "link":    ("link", "id"),
        "entity0": ("artist", "id"),
        "entity1": ("instrument", "id"),
    },
    "link": {
        "link_type": ("link_type", "id"),
    },
    "link_type": {
        "parent": ("link_type", "id"),
    },
}
```

To explore our loaded data (and also to verify that our schema definitions were correct), we programmed a loop that would generate comprehensive summary statistics for each table - including row count, missing count, missing percentage, duplicates, etc. We also used a handy “get_column_role” function to determine “roles” for each column in the tables, which would develop our understanding of the data.

Finally we generated helpful charts and tables to complete our data exploration. Most of these focused on value distributions in the tables, which would later inform decision-making on feature weighting, imputation, etc.

<img width="261" height="225" alt="Screenshot 2026-05-31 at 1 06 20 PM" src="https://github.com/user-attachments/assets/36550cf9-91f3-42dc-9ad6-fa03b32be3cd" />

<img width="154" height="139" alt="Screenshot 2026-05-31 at 1 06 32 PM" src="https://github.com/user-attachments/assets/7b1d6401-c053-460d-aca3-b49c8052c6d1" />

<img width="466" height="283" alt="Screenshot 2026-05-31 at 1 06 41 PM" src="https://github.com/user-attachments/assets/df6281c0-901a-4464-bb5b-27f29b84699c" />

### Preprocessing (using Spark) ([notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone4/musicbrainz_v4%20(Preprocessing).ipynb))

Now to assemble our dataset, we combined “release”, “artist_credit_name”, “artist”, “gender”, “area”, “release_group_tag” and ”tag” tables into a single table using SQL join logic and our foreign key map. Our table at the end of this step contained columns “release name”, “artist gender”, “area name”, “tag name”, and “tag count”.

```
# build table (release name, artist gender, area name, tag name, tag count)
model_df = (
    release_df
    .join(acn_df, release_df["release_artist_credit"] == acn_df["acn_artist_credit"], "inner")
    .join(artist_df, acn_df["acn_artist_id"] == artist_df["artist_id"], "inner")
    .join(gender_df, artist_df["artist_gender_id"] == gender_df["gender_id"], "left")
    .join(area_df, artist_df["artist_area_id"] == area_df["area_id"], "left")
    .join(rgt_df, release_df["release_release_group"] == rgt_df["rgt_release_group"], "left")
    .join(tag_df, rgt_df["rgt_tag_id"] == tag_df["tag_id"], "left")
    .select("release_name", "artist_gender", "area_name", "tag_name", "tag_count")
)
```

Our methodology is described in greater detail later in this document. During the data preparation process, however, we encountered issues when joining one of the tables, which prevented us from using our original approach for genre analysis. To address this limitation, we leveraged the "tag" table as an alternative source for genre exploration.

Because the tag data contained many entries that did not correspond to meaningful music genres, it was necessary to filter out these irrelevant tags, which we treated as noise within the context of our analysis. This filtering process was performed using the file "real_music_genres.csv," included in the project repository, which contains a curated list of valid music genres.

```
# ranking tags
tag_window = Window.partitionBy("release_name").orderBy(F.col("tag_count").desc_nulls_last())
ranked_df = model_df.withColumn("tag_rank", F.row_number().over(tag_window))

# aggregating on release_name, assigning top tag to "genre_name", storing the rest in "tags"
aggregated_df = (
    ranked_df
    .groupBy("release_name")
    .agg(
        F.first("artist_gender", ignorenulls=True).alias("artist_gender"),
        F.first("area_name", ignorenulls=True).alias("area_name"),
        F.max(F.when(F.col("tag_rank") == 1, F.col("tag_name"))).alias("genre_name"),
        F.collect_set(F.when(F.col("tag_rank") > 1, F.col("tag_name"))).alias("tags"),
    ).filter(F.col("genre_name").isNotNull())
)

# filter out garbage tags and genre names (using helper real_music_genres.csv file)
valid_genres = set(valid_genres_pd["Genre"].dropna().str.strip().str.lower())
aggregated_df = aggregated_df.filter(F.lower(F.col("genre_name")).isin(list(valid_genres)))
valid_genres_bc = spark.sparkContext.broadcast(valid_genres)
filter_tags_udf = F.udf(
    lambda tags: [t for t in (tags or []) if t and t.lower() in valid_genres_bc.value], ArrayType(StringType())
)
aggregated_df = aggregated_df.withColumn("tags", filter_tags_udf(F.col("tags")))
```

Following broad tag filtering, it was also necessary to distill the primary tags (our predictor variables) to a smaller pool that could be reasonably used in a classification problem. We settled on 19 genre buckets. Many of them grouped numerous genres together, such as “Folk / Singer-Songwriter”, “R&B / Soul / Funk”, while more common labels like “Rock” and “Pop” remained distinct buckets. These buckets did a great job reigning in the nuance of some of these releases. This was also accomplished using a .CSV file, this one called “genre_buckets_wide.csv”. It is also present in our project repo.

```
# map each genre_name to one of 19 broad buckets (using helper genre_buckets_wide.csv file)
buckets_pd = pd.read_csv("genre_buckets_wide.csv")
genre_to_bucket = {}
for bucket in buckets_pd.columns:
    for genre in buckets_pd[bucket].dropna():
        genre_clean = genre.strip().lower()
        if genre_clean:
            genre_to_bucket[genre_clean] = bucket

bucket_lookup_df = spark.createDataFrame([(k, v) for k, v in genre_to_bucket.items()], ["genre_key", "bucket_name"])
aggregated_df = (
    aggregated_df
    .join(F.broadcast(bucket_lookup_df),
          F.lower(F.col("genre_name")) == F.col("genre_key"), "left")
    .drop("genre_name", "genre_key")
    .withColumnRenamed("bucket_name", "genre_name")
    .filter(F.col("genre_name").isNotNull())
)
```

After the difficult task of tag filtering was finished, we moved onto imputing missing values in all columns, using insights gleaned from our EDA step. We also filtered the “areas” column, keeping only the top 120 most common areas (this resulted in negligible data loss).

Below is the resulting table, pre-encoding/indexing:

<img width="565" height="368" alt="Screenshot 2026-05-31 at 1 13 17 PM" src="https://github.com/user-attachments/assets/81158592-e531-4212-8027-e3c8ec461e7f" />

As is necessary in Spark ML, we had to convert this human-readable table into a computer-readable encoded/ indexed vector. Using CountVectorizer, we converted the tag list into a sparse binary vector onto itself - then we used StringIndexer to convert genre/tag, gender and area into indices, before feeding all of these into a VectorAssembler to create the final 1119-dimensional feature vector we would feed our model.

```
# using CountVectorizer to convert tag list into a sparse binary vector
cv = CountVectorizer(inputCol="tags", outputCol="tag_vector", minDF=2.0)
cv_model = cv.fit(aggregated_df)
vectorized_df = cv_model.transform(aggregated_df)
# using StringIndexer to convert genre/gender/area into a numeric index
genre_indexer = StringIndexer(inputCol="genre_name", outputCol="genre_index", handleInvalid="keep")
gender_indexer = StringIndexer(inputCol="artist_gender", outputCol="gender_index", handleInvalid="keep")
area_indexer = StringIndexer(inputCol="area_name", outputCol="area_index", handleInvalid="keep")
# putting all features into a single "features" vector using VectorAssembler
assembler = VectorAssembler(inputCols=["gender_index", "area_index", "tag_vector"], outputCol="features", handleInvalid="keep")
```

Final table after encoding/indexing:

<img width="557" height="337" alt="Screenshot 2026-05-31 at 1 13 52 PM" src="https://github.com/user-attachments/assets/a0f1838c-5f1f-4506-8663-e78f25c81fc5" />

### Model 1 (your first distributed model) ([notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone4/musicbrainz_v4%20(Model%201).ipynb))

We began with the customary train/test split, using an 80/20 ratio. Inverse-frequency class weighting helped us address some of the class imbalance we observed during the exploration step.

```
# train/ test split
train_df, test_df = final_df.randomSplit([0.8, 0.2], seed=42)
train_df = train_df.cache()
test_df = test_df.cache()
print(f"Train rows: {train_df.count():,}")
print(f"Test rows: {test_df.count():,}")

# inverse-frequency class weighting to deal with class imbalance
n_train = train_df.count()
n_classes = train_df.select("genre_index").distinct().count()

class_counts_df = (train_df.groupBy("genre_index").count().withColumn("class_weight",
        F.lit(float(n_train))/(F.lit(float(n_classes)) * F.col("count").cast("double"))
    ).select("genre_index", "class_weight"))

train_weighted = (train_df.join(F.broadcast(class_counts_df), on="genre_index", how="left").cache())
```

For the model itself, we chose to build 2 RandomForests with parameters numTrees = 50 and maxBins = 128, only with the first RF having maxDepth = 10, and the second having maxDepth = 12. Differentiating the models using only one parameter allows us to explore the sole impact of that parameter.

```
# build model 1
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="genre_index",
    weightCol="class_weight",
    numTrees=50,
    maxDepth=10,
    maxBins=128,
    seed=42,
)

rf_model = rf.fit(train_weighted)

# build model 2
rf2 = RandomForestClassifier(
    featuresCol="features",
    labelCol="genre_index",
    weightCol="class_weight",
    numTrees=50,
    maxDepth=12,
    maxBins=128,
    seed=42,
)

rf_model2 = rf2.fit(train_weighted2)
```

Finally, we outputted predictions from both models, evaluating for test accuracy, train accuracy, test F1 and train F1. We also evaluated per-class F1 scores, which allowed us to explore how consistently the models score across all classes. We will explore this further in the results section.

### Model 2 (PCA/SVD + clustering or supervised) ([notebook](https://github.com/rowheaton/ragus-wheaton-iribe-232/blob/milestone4/musicbrainz_v4%20(Model%202).ipynb))

Using the preprocessed dataframe from the preprocessing step, we scaled features and fit PCA with k = 100 to explore explained variance and cumulative variance, and to help determine ideal k-value before refitting the PCA on the dataset. The generated scree and cumulative explained variance plots provided visual helpers for identifying the k-value, which we settled on k = 50.

```
# scale features so each dim has unit var
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=False, withStd=True)
scaler_model = scaler.fit(final_df)
scaled_df = scaler_model.transform(final_df).select("release_name", "genre_index", "scaled_features").cache()
print(f"scaled feature dim: {scaled_df.first()['scaled_features'].size}")

# fit PCA with k=100 to see var curve before deciding if we need a smaller k
PCA_K_MAX = 100
pca_full = PCA(k=PCA_K_MAX, inputCol="scaled_features", outputCol="pca_features")
pca_full_model = pca_full.fit(scaled_df)

explained_var = np.array(pca_full_model.explainedVariance.toArray())
cum_var = np.cumsum(explained_var)
print(f"Top 5 component variances: {explained_var[:5]}")
print(f"Cumulative @ k=10: {cum_var[9]:.4f}")
print(f"Cumulative @ k=25: {cum_var[24]:.4f}")
print(f"Cumulative @ k=50: {cum_var[49]:.4f}")
print(f"Cumulative @ k=100: {cum_var[-1]:.4f}")

fig, axes = plt.subplots(1, 2, figsize=(13, 4))

axes[0].plot(range(1, PCA_K_MAX + 1), explained_var, marker='o', markersize=3)
axes[0].set_xlabel("Component"); axes[0].set_ylabel("Explained variance ratio")
axes[0].set_title("Scree plot"); axes[0].grid(alpha=0.3)

axes[1].plot(range(1, PCA_K_MAX + 1), cum_var, marker='o', markersize=3, color='C1')
for thresh in (0.80, 0.90, 0.95):
    axes[1].axhline(thresh, ls='--', alpha=0.4, color='gray')
    k_at = int(np.argmax(cum_var >= thresh)) + 1 if (cum_var >= thresh).any() else None
    if k_at:
        axes[1].axvline(k_at, ls=':', alpha=0.3, color='gray')
        axes[1].text(k_at + 1, thresh - 0.02, f"k={k_at} @ {int(thresh*100)}%", fontsize=9)
axes[1].set_xlabel("Number of components"); axes[1].set_ylabel("Cumulative explained variance")
axes[1].set_title("Cumulative explained variance"); axes[1].grid(alpha=0.3)

plt.tight_layout(); plt.show()

# pick smallest k that captures over ~90% variance
target_var = 0.90
PCA_K = int(np.argmax(cum_var >= target_var)) + 1 if (cum_var >= target_var).any() else 50
PCA_K = min(PCA_K, 50)
print(f"Selected k = {PCA_K} (captures {cum_var[PCA_K-1]:.3f} of variance)")
```

<img width="614" height="184" alt="Screenshot 2026-05-31 at 1 20 54 PM" src="https://github.com/user-attachments/assets/a51b4a9c-e559-4ef8-8314-f73bfc929485" />

After determining that k = 50 captured sufficient variance, we refit the PCA on the dataset using k = 50 to transform our giant, sparse, binary 1119-dimensional feature vector into a dense, much less noisy 50-dimensional vector to use for our second model.

```
# refitting PCA using the chosen k from above so the whole dataset is transformed into its reduced form
pca = PCA(k=PCA_K, inputCol="scaled_features", outputCol="pca_features")
pca_model = pca.fit(scaled_df)
reduced_df = pca_model.transform(scaled_df).select("release_name", "genre_index", "pca_features").cache()
print(f"Reduced feature dim: {reduced_df.first()['pca_features'].size}")
print(f"Row count: {reduced_df.count():,}")
```

As a sanity check, we conducted KMeans clustering on the reduced dataset and calculated the silhouette score and cluster purity. This is a measure to verify the accuracy of the groupings made by PCA.

```
# start Kmeans clustering on the reduced dataset
# we can use silhouette score to see how well the clusters are identified, thats the next chunk of code

n_clust = 19  # match number of genre buckets

kmeans = KMeans(featuresCol="pca_features", predictionCol="cluster", k=n_clust, seed=42, maxIter=20)
kmeans_model = kmeans.fit(reduced_df)
clustered_df = kmeans_model.transform(reduced_df).cache()

# silhouette score (squared Euclidean — Spark's default for ClusteringEvaluator)
silh_eval = ClusteringEvaluator(featuresCol="pca_features", predictionCol="cluster", metricName="silhouette", distanceMeasure="squaredEuclidean")
silh = silh_eval.evaluate(clustered_df)
print(f"Silhouette score (k={n_clust}): {silh:.4f}")

# cluster purity: in each cluster, what fraction belongs to the majority genre?
purity_df = (clustered_df.groupBy("cluster", "genre_index").count().withColumn("rank", F.row_number().over(Window.partitionBy("cluster").orderBy(F.col("count").desc()))))
top_per_cluster = purity_df.filter(F.col("rank") == 1).select("cluster", "count")
cluster_sizes = clustered_df.groupBy("cluster").count().withColumnRenamed("count", "cluster_size")
purity_joined = top_per_cluster.join(cluster_sizes, "cluster")
total_rows = clustered_df.count()
weighted_purity = (purity_joined.withColumn("contrib", F.col("count").cast("double") / F.lit(total_rows)).agg(F.sum("contrib")).first()[0])

print(f"Weighted cluster purity: {weighted_purity:.4f}")
print(f"(purity = fraction of points whose cluster's majority label matches their true label)")
```

<img width="463" height="388" alt="Screenshot 2026-05-31 at 1 22 00 PM" src="https://github.com/user-attachments/assets/f8f7e266-72a8-43f5-bf06-9aff933788d1" />

For the model we would train on this data, we initially opted for a LogisticRegression model with our previously defined class weighting, family = “multinomial”, maxIter = 50, regParam = 0.01 and elasticNetParam = 0). This produced a modestly superior result to our RF models without PCA, but we also experimented with an XGBoost model, with maxDepth = 5, n_estimators = 50, learning_rate = 0.3, subsample = 0.8, and colsample_bytree = 0.8; this produced our overall best result. The results will be explored in depth in the results section.

```
# new xgboost model on the PCA reduced features
from xgboost.spark import SparkXGBClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

train_xgb, test_xgb = reduced_df.select(
    "genre_index", 
    "pca_features"
).randomSplit([0.8, 0.2], seed=42)

xgb = SparkXGBClassifier(
    features_col="pca_features",
    label_col="genre_index",
    prediction_col="prediction",
    num_class=19,
    max_depth=5,
    n_estimators=50,
    learning_rate=0.3,
    subsample=0.8,
    colsample_bytree=0.8
)

xgb_model = xgb.fit(train_xgb)

xgb_preds = xgb_model.transform(test_xgb)
```


## Results 🧮

### Model 1 (your first distributed model)

Our first RandomForest model with maxDepth = 10 performed modestly, scoring 0.3693 train accuracy, 0.3677 test accuracy, 0.3730 train weighted F1 and 0.3716 test weighted F1. Looking at the per-class F1 scores, we can see that the model performs well on a handful of genres such as Electronic/Dance (0.59), Jazz (0.62), Hip-Hop/Rap (0.54), Metal (0.54), and Reggae/Ska/Dub (0.56), while performing poorly on genres like Rock (0.2), Pop (0.12) and World music (0.05). These per-class F1 scores show a great deal of inconsistency across classes, and furthermore, it doesn’t seem as though this inconsistency is related to the frequency of classes (classes like Electronic/Dance and Pop are both heavily favored in the data, but have vastly different per-class F1 score).

*First RandomForest model with maxDepth = 10:*

<img width="320" height="335" alt="Screenshot 2026-05-31 at 1 22 32 PM" src="https://github.com/user-attachments/assets/43309bf9-2db9-49c3-848f-3b48b74cd5cc" />

Our second RandomForest model with maxDepth = 12 performed very similarly to the first model, with 0.3766 train accuracy, 0.3737 test accuracy, 0.3789 train weighted F1, and 0.3754 test weighted F1. This very slight performance boost across the board shows that for an RF model on this kind of sparse data, increasing tree depth allows the model to capture just a little more nuance from each datapoint. Per-class evaluations show the same pattern as the first model.

*Second RandomForest model with maxDepth = 12:*

<img width="312" height="319" alt="Screenshot 2026-05-31 at 1 23 21 PM" src="https://github.com/user-attachments/assets/515c2fe5-a9b8-4832-9209-a0e12189fbd5" />

Here is a sample prediction, generated by the first maxDepth = 10 RF model. From this small sample, we can see some of the matches (notice the Jazz pieces being predicted correctly, with genre_index = 3.0). However the shortcomings of the model and the sub-optimal performance is visible here.

<img width="282" height="328" alt="Screenshot 2026-05-31 at 1 28 08 PM" src="https://github.com/user-attachments/assets/cb42ae94-35bc-4f7c-bada-aa63d350d27f" />

### Model 2 (PCA/SVD + clustering or supervised)

The final XGBoost model we built on top of the PCA-reduced data performed well, scoring 0.62 train accuracy, 0.61 test accuracy, 0.611 train weighted F1 and 0.602 test weighted F1 - demonstrating that the model correctly classifies releases a majority of the time. Looking at the per-class F1 scores, we also see that the model performs consistently well across most classes - another advantage of this final configuration. The only classes that the model still performs poorly on are the uncommon classes, like “Other” and “Gospel/Christian/Spiritual”, further confirming that the dimensionality reduction from PCA is very effective in streamlining the prediction task.

Before training the final XGBoost model, we also trained a simple Logistic Regression model on the PCA-reduced data, which also performed well: roughly 0.5 accuracy/weighted F1 on both train and test sets.

*Final XGBoost model with PCA:*

<img width="377" height="337" alt="Screenshot 2026-05-31 at 1 28 29 PM" src="https://github.com/user-attachments/assets/baca43d0-ad2e-4aa8-bb30-ed89b0bc11fd" />


## Discussion 🗣️

### Data Exploration

The data exploration stage began with downloading it into our environment and "assembling" it. The MusicBrainz site has two ways to download the data - as dumps of JSON files or dumps of .TSV files (basically, text files). Neither were clearly defined table formats like .CSV, we first needed to explicitly define schema in our code for each of the tables we were interested in, prior to ingesting any data into these schema to generate the tables.

The schema building process took a lot of trial and error because the .TSV files are difficult to read with the human eye. We tested various SQL queries and iterated through trial and error to figure out whether our schema were correct.
For the exploration itself, we wanted to visualize the features we intended on using in training, such as artist gender, tags, and labels. We specifically cared about distributions and skew in some of these tables, since this would determine if weighting/skew is necessary in imputation, model training, etc.

### Preprocessing (using Spark)

After exploratory analysis and testing, we discovered an important issue with the MusicBrainz schema: directly linking artists to genres through the l_artist_genre linking table was not practical because the table is almost empty. Joining through this relationship produced a dataset of only about 400 rows, which was too small for meaningful model training. So instead, we thought to leverage the richer tagging system in MusicBrainz to address this issue. Tags and genres already have substantial semantic overlap (tags such as “rock”, “jazz”, or “metal” often function as genre labels), and using tags dramatically increases the amount of usable data. This alternative approach produced a dataset of approximately 1.5 million rows.

Using this strategy, we constructed a table containing release_name, artist_name, area_name, label_name, and tag_name (a list of tags associated with the release). The first tag in the tag list was selected and treated as the target variable genre_name (since the first tag is often the most representative label for the release). The remaining tags were preserved as additional contextual features and stored as a vector under tags. The model predicts genre_name (derived from the first tag) using features such as artist name, geographic area, record label, and associated tags, while release_name primarily serves as a relational anchor connecting the metadata together rather than as a predictive feature itself.

This “tag” approach was clever, but in hindsight, there are some caveats. First of all, we made the bold assumption that the most relevant tag would always be an indicator for a release’s genre. We knew that tags were ordered from most to least relevant, but a tag being most relevant does not necessarily mean that it is the same as its genre. It is possible that this assumption introduced unnecessary noise into our model. We should also acknowledge that the tag filtering, which we deemed necessary for simplifying the problem, did take a lot of the nuance out of the dataset. Another idea is to have treated this as a multi-label classification problem instead of a single-label classification problem. The XGBoost already uses multi:softprob to generate a probability distribution, so it’s not a huge lift to use, say, top 3 probabilities instead of just the top probability.

At this stage, it also would have made sense to include “release_year” as a feature. Genre distributions shift massively over time, and this feature was abundant in the data. 

### Model 1 (your first distributed model)

We chose RandomForest as our first model because it was a model that we already had familiarity with. In the initial drafts of the milestone 3 notebook, we defined a parameter grid and used GridSearchCV to determine optimal parameters.

The somewhat lackluster performance of both of the RF models was kind of to be expected, given that RF considers only one feature at a time for each split, meaning weak tag indicators struggle to create a useful decision boundary. It might have made more sense to just train an XGBoost model right off the bat, since XGBoost has built-in regularization and sparsity-aware split finding.

### Model 2 (PCA/SVD + clustering or supervised)

We decided to go with dimensionality reduction as the next step because the raw tag vector is high dimensional and sparse, which Random Forest doesn't handle the best. PCA projects the features into a small dense space where each component captures cooccurrence structure across many tags. PCA pairs well with a linear model like Logistic Regression, so the initial model we built was a Logistic Regression model. We also tried to build an RF on the PCA-reduced data but we were running into memory errors (probably since PCA requires an RF to then aggregate across all 50 features, which is a way bigger lift than the first RF model).


## Conclusion 🔒

*Next steps:*
We can safely say that we have a model that can accurately predict a release's genre using only some metadata, a majority of the time. If we wanted to go further and pursue the ambitious goal we set out in our abstract, which was to build a recommendation system, a model like this would serve as a solid foundation to build on.

A good place to start with a recommendation system would be to run every release in the MusicBrainz dataset through our model and extract the genre probability distribution vector for each, then use something like cosine similarity to calculate the similarity between each release’s probability vector with all others. This would provide a genre-aware similarity metric built entirely on metadata. With this similarity metric, we could start profiling specific listeners using data from the accompanying “ListenBrainz” dataset (which, you guessed it, contains extensive listener metadata) using a similar 19-dimensional vector, representing genre preferences weighted by their listening history.

*What we learned:*
This was a complex project. Given the complexity of the data and task, we had to adapt our approach and expectations at each step of the process to meet each milestone. We learned that big data processing requires solid infrastructure: there is certainly no way we could have completed this project without distributed computing given the sheer mass of the data at hand. Resource management is critical. When developing locally on bite-sized datasets, it’s hard to appreciate how haphazardly one can code and not run into memory or compute issues - this was certainly not the case for this project. 

Using distributed computing also taught us about the more strict data formatting requirements in Spark ML. We had to take the additional steps of encoding and vectorizing our training data before feeding it into our model, something that isn’t often necessary for simpler systems.

Most of all, this project taught us about how important it is to coordinate complex tasks effectively with a team. We were only able to complete this project because each group member made the effort to play their part. Being able to delegate tasks is one thing, but having an environment where each team member is able to bear the load when other team members are overwhelmed made this project a delight to work on.


## Statement of Collaboration 🤹

- Tom: Everyone in the team was responsible and contributed equally to the project. We met regularly over the course of the entire project to discuss ideas/approach, delegate tasks in alignment with our own respective schedules, and keep each other accountable. All group members contributed equally to building out the code and written sections and deserve full credit for this project!

- Roxy: Our team approached this project as a shared responsibility with each member involved in every stage of development. We had frequent discussions which allowed us to share ideas, make decisions collectively, and divide work in a way that accommodated everyone's availability. The final product reflects the combined efforts of the entire group, as all members contributed substantially to both the implementation and documentation.

- Sergio: We collaborated throughout every stage of the project and maintained consistent communication throughout each milestone. As a team, we discussed project goals, developed approaches together, and delegated responsibilities based on availability and project needs. We regularly worked together to solve coding and documentation issues, review progress, and refine our implementation. It was a valuable experience to learn from one another and collaborate on this project!
