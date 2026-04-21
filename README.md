# ragus-wheaton-iribe-232


### Abstract
We have selected the musicbrainz.db dataset, a community-maintained, open source encyclopedia of music information with over 60GB of native SQL data in Postgres with well-defined schema. The tables include data for artists, genres, instruments, labels, mediums, releases, recordings, etc. Our core interest is to explore the relationships within this data and how they shape listener taste preferences. Model 1 will utilize the K-Nearest Neighbors approach on meta vectors which defines exactly how related two songs are based on their “DNA” (instruments, genres, labels, etc). Exploring how this model establishes these groups will give a solid foundation for our primary interest in Model 2, which aims to build a recommendation system that produces a k-length list of recommended releases for a given listener ID. To support this, a partner dataset called "ListenBrainz" contains extensive listener metadata that can be combined with the MusicBrainz data. Given the complexity of this problem and the volume of measurable relationships between variables in the MusicBrainz data, a neural network approach, likely a Two-Tower MLP or GNN, will probably be warranted. In the case of issues connecting the ListenBrainz data, we may opt to synthesize the listener data, which will make for another interesting challenge.

[Data download here](https://metabrainz.org/datasets/postgres-dumps#musicbrainz)

### SDSC Expanse Environment Setup
In Expanse terminal:

- To create the MusicBrainz_project directory:

mkdir -p ~/musicbrainz_project/raw_data

cd ~/musicbrainz_project/raw_data

- To download the data dumps

wget https://data.metabrainz.org/pub/musicbrainz/data/fullexport/20260418-002325/mbdump.tar.bz2

wget https://data.metabrainz.org/pub/musicbrainz/data/fullexport/20260418-002325/mbdump-derived.tar.bz2

- To extract text from data dumps

tar -xvf mbdump.tar.bz2

tar -xvf mbdump-derived.tar.bz2

- SparkSession configuration

Account: uci157
Partition: shared (default)
Cores: 4
Memory per node: 32GB
Environment modules to be loaded (important): cpu/0.15.4,anaconda3/2020.11
Executor instances = 4 - 1 = 3
Executor memory = (32 - 3) / 3 = 10GB per executor

For whatever reason simply typing cpu,anaconda3 would give me an error - it only worked when explicitly typing out the version numbers. Not sure if everyone else had to do this
