import json
from pandas import json_normalize
import itertools

def addSeasonSlug(x,y):
    x['seasonSlug'] = y
    return x


def mapSeasons(x):
    if x['seasons'] :
        seasons = [s['slug'] for s in x['seasons']]
        data = [ addSeasonSlug(x.copy(),y) for y in seasons] # make sure x is a new object
        return data
    else:
        x['seasonSlug'] = ''
        return [x]


def mapData(data):
    return [ {
        'country' :x['country'],
        'description' :x['description'],
        'drm' :x['drm'],
        'episodeCount' :x['episodeCount'],
        'genre' :x['genre'],
        'imageURL' :x['image']['showImage'],   
        'language' :x['language'],
        'seasonSlug' :x['seasonSlug'],
        'title' :x['title'],
        'slug' :x['slug'],
        'tvChannel':x['tvChannel']
    } 
    for x in data
    ]

def getCsvFromJson(data):
    data = data['payload']
    data = [ mapSeasons(x) for x in data ] 
    data  = list(itertools.chain(*data))
    data = mapData(data)
    dataDf = json_normalize(data)
    return dataDf


if __name__ == '__main__':
    # load sample json
    f = open('./__tests__/sample.json')
    sample = json.load(f)
    f.close()
    getCsvFromJson(sample).to_csv('__tests__/output.csv',index=None,header=True)
