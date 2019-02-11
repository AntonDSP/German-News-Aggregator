import json
from src.methods import aclust
from src.utils import mongodb
import dateutil.parser
import datetime
from src.utils import data_connector

class CluterModel:
    def __init__(self, model_name, threshold, similarity_measure, time_range, model_params):
        self.model_name=model_name
        self.threshold=threshold
        self.similarity_measure=similarity_measure
        self.model_params=model_params
        self.time_range=time_range
        if self.model_name=='aclust':
            self.max_dist=self.model_params['MAX_DIST']
            self.max_skip=self.model_params['MAX_SKIP']
            self.linkage=self.model_params['LINKAGE']


    def assign_existing_clusters(self, publications):
        # Check for every publication if similar publications within time range are already exists and assign the same cluster
        mongo_coll=mongodb.connect2db(db='german_news').get_collection(name='publications')
        new_l_publications=list()
        for publication in publications:
            publication.threshold=self.threshold
            publication.similarity_measure=self.similarity_measure
            start=dateutil.parser.parse(publication.content['published'])-datetime.timedelta(days=self.time_range)
            end = dateutil.parser.parse(publication.content['published']) + datetime.timedelta(days=self.time_range)
            publications_within_time_range=mongo_coll.find({'cluster':{'$exists':True},'published':{'$lt':end.isoformat(), '$gte':start.isoformat()}})
            nearest_clusters = {}
            for publication_w_time_range in publications_within_time_range:
                if publication.is_correlated(publication_w_time_range):
                    distance=publication.distance(publication_w_time_range)
                    nearest_clusters=nearest_clusters.update({publication_w_time_range['cluster']:distance})
            if bool(nearest_clusters):
                publication.content['cluster']=max(nearest_clusters, key=nearest_clusters.get)
            else:
                publication.content['cluster']=''
          #  print("Publication_id: "+publication.content['publication_id']+"Cluster_id: " + publication.content['cluster'])
            new_l_publications.append(publication)
        return new_l_publications

    def create_news_clusters(self, publications):
        all_other_publications = [publication for publication in publications if publication.content['cluster'] == '']
        if self.model_name=='aclust':
            clusters = (cluster for cluster in aclust.mclust(all_other_publications, max_dist=self.max_dist, max_merge_dist=self.max_dist, max_skip=self.max_skip, linkage=self.linkage))
        else:
            print("The cluster model is not implemented yet")
            clusters=None
        new_l_clusters=list(clusters)
        return new_l_clusters

    def assign_clusters(self, publications):
        l_publications=self.assign_existing_clusters(publications)
        l_clusters = self.create_news_clusters(l_publications)
        for cluster in l_clusters:
            i=0
            for publ in cluster:
                if i==0:
                    cluster_id=publ.content['publication_id']
                i=i+1
                for l_publication in l_publications:
                    if publ.content['publication_id']==l_publication.content['publication_id']:
                        l_publication.content['cluster']=cluster_id
                    #    print("Publication_id: " + l_publication.content['publication_id'] + "Cluster_id: " + l_publication.content['cluster'])
        return l_publications



if __name__ == '__main__':
    print("Run...")
    config=default_config
    news_stream= data_connector.NewsReader(config['SOURCE']).read_news()
    mclust(news_stream, max_dist=400, max_skip=1)
    print("Finished")