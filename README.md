# German-News-Aggregator
Aggregation of published German news in real-time for further text data exploration, which includes the following components:
- Named entity recognition
- Keyphrases extraction
- Topics detection
- Sentiment analysis
- Clustering of news publications
- Text summarization for created clusters 

This application uses for the clustering Aclust algorithm: https://github.com/brentp/aclust

Installation: 
Developed and tested with Python 3.6
Install all dependencies from requirements.txt

Usage:
The application can currently read and write streams from mongoDB and Kafka.
The connectivity configurations for data sources and targets are specified in project_config.json
Particular data sources and targets are specified in flow_config.json
The configuration for German news aggregator components are specified in news_aggregator_config.json 
