#!/usr/bin/env python
# coding: utf-8

# In[367]:


# Nos traemos la base de las compañías

from pymongo import MongoClient
client = MongoClient("localhost:27017")
db = client.get_database("ironhack")
c = db.get_collection("companies")


# In[554]:


import os
import json
import requests
import pandas as pd
import pyjsonviewer
import geopandas
import shapely.geometry
from pymongo import MongoClient,GEOSPHERE
from dotenv import load_dotenv
import folium
from folium import Choropleth, Circle, Marker, Icon, Map
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
load_dotenv()
clave = os.getenv(("token"))


# In[571]:


# Configuramos la clave de maps

import gmaps
gmaps.configure(api_key=clave)


# In[572]:


# Nos traemos la base de las compañías

from pymongo import MongoClient
client = MongoClient("localhost:27017")
db = client.get_database("ironhack")
c = db.get_collection("companies")


# In[573]:


# Y nos filtramos para buscar compañías con características similares a la nuestra
# Y observamos que existen tres ubicaciones, una en Nueva York, otra en Los Ángeles, y otra en el estado de Georgia

proy = {'_id': 0, "offices.latitude": 1,"offices.longitude": 1, "offices.address1": 1}
cond1 = {"category_code": "games_video"}
cond2 = {"founded_year": {"$gte": 2000}}
cond3 = {"number_of_employees":{"$gt": 60}}
cond4 = {"number_of_employees":{"$lt": 100}}
cond5 = {"offices.country_code": 'USA'}
list(c.find({"$and": [cond1,cond2,cond3,cond4,cond5]},proy))


# In[577]:


# Nos guardamos las coordenadas

NYCO = [40.743877, -73.98618]
LACO = [34.09316, -118.37833]
GACO = [34.17036, -84.28746]
LATNY = 40.743877
LONGNY = -73.98618
LATLA = 34.09316
LONGLA = -118.37833
LATGA = 34.17036
LONGGA = -84.28746


# In[121]:


# De los distintos criterios posibles, consideremos para la ubicación de nuestra empresa la proximidad a:
# Starbucks, aeropuertos y escuelas

# asignamos keyword para buscar 
SB = "starbucks"
AP = "airport"
SC = "school"


# In[124]:


radios = 500 # radio en metros para la busqueda de starbucks
radioa = 20000 # radio en metros para la busqueda de aeropuertos
radioe = 2000 # radio en metros para la busqueda de escuelas


# In[277]:


# función para sacar la latitud
def latt (df_col):
    return df_col['location']['lat']


# In[278]:


# función para sacar la longitud
def long (df_col):
    return df_col['location']['lng']


# In[279]:


# función para sacar el typepoint
def typePoint (lng,lat):
    return {'type': 'Point', 'coordinates': [lng, lat]}


# In[179]:


# Buscamos los Starbucks en NY

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATNY},{LONGNY}&radius={radios}&keyword={SB}&key={clave}"

payload={}
headers = {}

response11 = requests.request("GET", url, headers=headers, data=payload)

print(response11.text)


# In[321]:


# limpiamos resultados
a = response11.json()['results']
dfa = pd.DataFrame(a)
dfaa = dfa.filter(items=['name', 'geometry'])


# In[322]:


# aplicamos funciones y limpiamos resultados
dfaa['latitude'] = dfaa.geometry.apply(latt)
dfaa['longitude'] = dfaa.geometry.apply(long)
dfaa["points"] = dfaa.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfaa.drop(["geometry"] , axis=1, inplace=True)
dfaa.insert(0, 'type', 'SB')


# In[126]:


# Aeropuertos NY

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATNY},{LONGNY}&radius={radioa}&keyword={AP}&key={clave}"

payload={}
headers = {}

response12 = requests.request("GET", url, headers=headers, data=payload)

print(response12.text)


# In[326]:


# limpiamos resultados
b = response12.json()['results']
dfb = pd.DataFrame(b)
dfbb = dfb.filter(items=['name', 'geometry'])


# In[327]:


# aplicamos funciones y limpiamos resultados
dfbb['latitude'] = dfbb.geometry.apply(latt)
dfbb['longitude'] = dfbb.geometry.apply(long)
dfbb["points"] = dfbb.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfbb.drop(["geometry"] , axis=1, inplace=True)
dfbb = dfbb.head(4)
dfbb.insert(0, 'type', 'Airport')


# In[127]:


# Escuelas NY

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATNY},{LONGNY}&radius={radioe}&keyword={SC}&key={clave}"

payload={}
headers = {}

response13 = requests.request("GET", url, headers=headers, data=payload)

print(response13.text)


# In[329]:


# limpiamos resultados
c = response13.json()['results']
dfc = pd.DataFrame(c)
dfcc = dfc.filter(items=['name', 'geometry'])


# In[330]:


# aplicamos funciones y limpiamos resultados
dfcc['latitude'] = dfcc.geometry.apply(latt)
dfcc['longitude'] = dfcc.geometry.apply(long)
dfcc["points"] = dfcc.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfcc.drop(["geometry"] , axis=1, inplace=True)
dfcc.insert(0, 'type', 'School')


# In[128]:


# Starbucks LA

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATLA},{LONGLA}&radius={radios}&keyword={SB}&key={clave}"
payload={}
headers = {}

response21 = requests.request("GET", url, headers=headers, data=payload)

print(response21.text)


# In[338]:


# limpiamos resultados
d = response21.json()['results']
dfd = pd.DataFrame(d)
dfdd = dfd.filter(items=['name', 'geometry'])


# In[339]:


# aplicamos funciones y limpiamos resultados
dfdd['latitude'] = dfdd.geometry.apply(latt)
dfdd['longitude'] = dfdd.geometry.apply(long)
dfdd["points"] = dfdd.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfdd.drop(["geometry"] , axis=1, inplace=True)
dfdd.insert(0, 'type', 'SB')


# In[231]:


# Aeropuertos LA

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATLA},{LONGLA}&radius={radioa}&keyword={AP}&key={clave}"
payload={}
headers = {}

response22 = requests.request("GET", url, headers=headers, data=payload)

print(response22.text)


# In[335]:


# limpiamos resultados
e = response22.json()['results']
dfe = pd.DataFrame(e)
dfee = dfe.filter(items=['name', 'geometry'])


# In[336]:


# aplicamos funciones y limpiamos resultados
dfee['latitude'] = dfee.geometry.apply(latt)
dfee['longitude'] = dfee.geometry.apply(long)
dfee["points"] = dfee.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfee.drop(["geometry"] , axis=1, inplace=True)
dfee = dfee.head(6)
dfee.insert(0, 'type', 'Airport')


# In[232]:


# Escuelas LA

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATLA},{LONGLA}&radius={radios}&keyword={SC}&key={clave}"

payload={}
headers = {}

response23 = requests.request("GET", url, headers=headers, data=payload)

print(response23.text)


# In[332]:


# limpiamos resultados
f = response23.json()['results']
dff = pd.DataFrame(f)
dfff = dff.filter(items=['name', 'geometry'])


# In[333]:


# aplicamos funciones y limpiamos resultados
dfff['latitude'] = dfff.geometry.apply(latt)
dfff['longitude'] = dfff.geometry.apply(long)
dfff["points"] = dfff.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfff.drop(["geometry"] , axis=1, inplace=True)
dfff.insert(0, 'type', 'School')


# In[334]:


dfff.head()


# In[132]:


# Starbucks GA

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATGA},{LONGGA}&radius={radios}&keyword={SB}&key={clave}"

payload={}
headers = {}

response31 = requests.request("GET", url, headers=headers, data=payload)

print(response31.text)


# In[133]:


# Aeropuertos GA

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATGA},{LONGGA}&radius={radioa}&keyword={AP}&key={clave}"

payload32={}
headers32 = {}

response32 = requests.request("GET", url, headers=headers32, data=payload32)

print(response32.text)


# In[342]:


# limpiamos resultados
h = response32.json()['results']
dfh = pd.DataFrame(h)
dfhh = dfh.filter(items=['name', 'geometry'])


# In[343]:


# aplicamos funciones y limpiamos resultados
dfhh['latitude'] = dfhh.geometry.apply(latt)
dfhh['longitude'] = dfhh.geometry.apply(long)
dfhh["points"] = dfhh.apply(lambda x: typePoint(x['latitude'], x['longitude']), axis = 1)
dfhh.drop(["geometry"] , axis=1, inplace=True)
dfhh = dfhh.head(1)
dfhh.insert(0, 'type', 'Airport')


# In[344]:


dfhh


# In[373]:


# Escuelas GA

import requests

url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={LATGA},{LONGGA}&radius={radioe}&keyword={SC}&key={clave}"

payload={}
headers = {}

response33 = requests.request("GET", url, headers=headers, data=payload)

print(response33.text)

# No existen ni escuelas ni Starbucks cerca de la ubicación de Georgia, por lo que la descartamos como opción


# In[439]:


# Concatenamos las escuelas, aeropuertos, y Starbucks de NY
NY = pd.concat([dfaa, dfbb, dfcc,],ignore_index=True)


# In[440]:


# Concatenamos las escuelas, aeropuertos, y Starbucks de LA
LA = pd.concat([dfdd, dfee, dfff,],ignore_index=True)


# In[ ]:


# En la siguientes celdas introducimos los datos en Mongo


# In[442]:


NY = geopandas.GeoDataFrame(NY, geometry=geopandas.points_from_xy(NY.longitude, NY.latitude))
LA = geopandas.GeoDataFrame(LA, geometry=geopandas.points_from_xy(LA.longitude, LA.latitude))


# In[443]:


NY.drop(["points"] , axis=1, inplace=True)
LA.drop(["points"] , axis=1, inplace=True)


# In[444]:


collectionNY = client.ironhack["NY"]
collectionLA = client.ironhack["LA"]


# In[446]:


collectionNY.create_index([("geometry", GEOSPHERE)])
collectionLA.create_index([("geometry", GEOSPHERE)])


# In[447]:


NY['geometry']=NY['geometry'].apply(lambda x:shapely.geometry.mapping(x))
LA['geometry']=LA['geometry'].apply(lambda x:shapely.geometry.mapping(x))


# In[448]:


NY = NY.to_dict(orient='records')
LA = LA.to_dict(orient='records')


# In[449]:


collectionNY.insert_many(NY)
collectionLA.insert_many(LA)


# In[451]:


# Codificamos las colecciónes
MNY = db.get_collection("NY")
MLA = db.get_collection("LA")


# In[514]:


# Establecemos las coordenadas en tipo point
DNY = {'type': 'Point', 'coordinates': [-73.98618, 40.743877]}
DLA = {'type': 'Point', 'coordinates': [-118.37833, 34.09316]}


# In[543]:


# Realizamos las busquedas acotando los radios
SearchNYSB = {"type" :"SB", "geometry": {"$near": {"$geometry": DNY, "$maxDistance": 250}}} #Starbuck en NY radio 250M
SearchLASB = {"type" :"SB", "geometry": {"$near": {"$geometry": DLA, "$maxDistance": 250}}} #Starbuck en LA radio 250M
SearchNYSC = {"type" :"School", "geometry": {"$near": {"$geometry": DNY, "$maxDistance": 1000}}} #Escuelas en NY radio 1000M
SearchLASC = {"type" :"School", "geometry": {"$near": {"$geometry": DLA, "$maxDistance": 1000}}} #Escuelas en LA radio 1000M
SearchNYAP = {"type" :"Airport", "geometry": {"$near": {"$geometry": DNY, "$maxDistance": 15000}}} #Aeropuertos en NY radio 15.000M
SearchLAAP = {"type" :"Airport", "geometry": {"$near": {"$geometry": DLA, "$maxDistance": 15000}}} #Aeropuertos en NLA radio 15.000M


# In[548]:


# Convertimos las busquedas en listas y en data frames

NYSB = list(MNY.find(SearchNYSB))
LASB = list(MLA.find(SearchLASB))
NYSC = list(MNY.find(SearchNYSC))
LASC = list(MLA.find(SearchLASC))
NYAP = list(MNY.find(SearchNYAP))
LAAP = list(MLA.find(SearchLAAP))

DFNYSB = pd.DataFrame(NYSB)
DFLASB = pd.DataFrame(LASB)
DFNYSC = pd.DataFrame(NYSC)
DFLASC = pd.DataFrame(LASC)
DFNYAP = pd.DataFrame(NYAP)
DFLAAP = pd.DataFrame(LAAP)


# In[ ]:


# Consideramos más relevantes los aeropuertos, que las escuelas y éstas que los Starbucks
# Por lo que dentro de los resultados obtenidos, multiplicamos por 3 los aeropuertos encontrados, por 2 las escuelas,
# por 1 los Starbucks, y sumamos para ver que ciudad obtiene mayor puntuación


# In[574]:


PointsNY = len(NYAP)*3+len(NYSC)*2+len(NYSB)
PointsNY


# In[575]:


PointsLA = len(LAAP)*3+len(LASC)*2+len(LASB)
PointsLA


# In[ ]:


# Observamos que LA tiene una puntuación ligeramente superior a NY. Por lo que sería ubicación elegida

# A continuación pintamos los mapas para facilitar la visualización


# In[549]:


# Juntamos los DF
MAPANY = pd.concat([DFNYSB, DFNYSC, DFNYAP],ignore_index=True)
MAPALA = pd.concat([DFLASB, DFLASC, DFLAAP],ignore_index=True)


# In[595]:


# Establecemos el punto de partida de los mapas
mapNY = folium.Map(location = NYCO, zoom_start = 15)
mapLA = Map(location = LACO, zoom_start = 15)

# Añadimos la etiqueta de ubicación para las oficinas
etiquetaNY = folium.Marker(location=NYCO, tooltip = "Oficina NY")
etiquetaNY.add_to(mapNY)

etiquetaLA = folium.Marker(location=LACO, tooltip = "Oficina LA")
etiquetaLA.add_to(mapLA)


# In[604]:


# Iteramos sobre el mapa de NY y le añadimos marcadores

for i,row in MAPANY.iterrows():
    distrito = {"location": [row["latitude"], row["longitude"]], "tooltip": row["type"]}
    
    if row["type"] == "SB":
        icono = Icon(color = "green",
                     prefix="fa",
                     icon="coffee",
                     icon_color="black"
        )
    elif row["type"] == "Airport":
        icono = Icon(color = "blue",
                     prefix="fa",
                     icon="plane",
                     icon_color="black")
        
    elif row["type"] == "School":
        icono = Icon(color = "red",
                     prefix="fa",
                     icon="book",
                     icon_color="black")
        
    mark = Marker(**distrito, icon=icono)
    mark.add_to(mapNY)
mapNY


# In[605]:


# Iteramos sobre el mapa de LA y le añadimos marcadores

for i,row in MAPALA.iterrows():
    distrito = {"location": [row["latitude"], row["longitude"]], "tooltip": row["type"]}
    
    if row["type"] == "SB":
        icono = Icon(color = "green",
                     prefix="fa",
                     icon="coffee",
                     icon_color="black"
        )
    elif row["type"] == "Airport":
        icono = Icon(color = "white",
                     prefix="fa",
                     icon="plane",
                     icon_color="black")
        
    elif row["type"] == "School":
        icono = Icon(color = "red",
                     prefix="fa",
                     icon="book",
                     icon_color="black")
        
    mark = Marker(**distrito, icon=icono)
    mark.add_to(mapLA)
mapLA


# In[ ]:




