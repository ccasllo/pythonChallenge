#! /usr/bin/env python
# -*- coding: utf-8 -*-
""" Aplicación para resolver el Challenge Python L1
    Desarrollado por Carlos Alberto Castillo
    version 1.0 
    Bitacora: 
    fecha: 2021-02-27   observacion: Version 1      por: Carlos Castillo
"""

import requests
import pandas as pd
import json
import hashlib
from datetime import datetime as dt
import sqlite3
from os import path,remove #validar si existen archivos
import logging
import argparse # agregar parametros por script 

# Para Ejecutar pruebas unitarias se debe agregar el parametro -T y para log el parametro -L
# ejemplo
# cd %path del proyecto%
# python.exe pythonchallenge.py -L

parser = argparse.ArgumentParser()
parser.add_argument("-L", "--logs", help="Crea archivo de log en la carpeta /logs/ ", action="store_true")
parser.add_argument("-T", "--Test", help="Ejecuta los test unitarios", action="store_true" )
args = parser.parse_args()

logs=args.logs
unitTest=args.Test

def ahora():
  return str(dt.now().strftime("%Y-%m-%d %H:%M:%S:%f"))

archivoLog=ahora().replace('-','').replace(':','').replace(' ','')+'.log'
fichero_log='./logs/'+archivoLog

if logs:
  logging.basicConfig(level=logging.DEBUG,
                      format='%(asctime)s : %(levelname)s : %(message)s',
                      filemode = 'w',
                      filename = fichero_log,
                      )
  logging.debug('Inicio  {0}'.format(ahora()))


# 1. De https://rapidapi.com/apilayernet/api/rest-countries-v1, obtenga todas las regiones existentes.

def extraeRegiones():
  """
  Retorna las regiones 
  >>> regiones = extraeRegiones()
  >>> respuesta=  {'', 'Polar', 'Africa', 'Europe', 'Asia', 'Americas', 'Oceania'}
  >>> [ True for x in regiones if x in respuesta ]
  [True, True, True, True, True, True, True]
  """
  url = "https://restcountries-v1.p.rapidapi.com/all"
  headers = {
      'x-rapidapi-key': "a40867b8aemsh67103447d5b31a9p15f455jsn556d45d7d10d",
      'x-rapidapi-host': "restcountries-v1.p.rapidapi.com"
      }
  response = requests.request("GET", url, headers=headers)
  respuestaJson = response.json()
  df=pd.DataFrame(data=respuestaJson)
  regionesExistentes=set(df['region'])
  if logs:
    logging.debug('Se encontraron las siguientes regiones: {}'.format(regionesExistentes))
  return regionesExistentes

#2. De https://restcountries.eu/ Obtenga un pais por region apartir de la region optenida del punto 1.
#3 De https://restcountries.eu/ obtenga el nombre del idioma que habla el pais y encriptelo con SHA1
#4 En la columna Time ponga el tiempo que tardo en armar la fila (debe ser automatico)
def regionPaisIdioma(region):
  """
  Función que retorna un pais por region, y encripta el idioma
  >>> v1,v2,v3,v4,v5 = regionPaisIdioma('Americas')
  >>> print(len(v2)>1)  
  True
  """
  date1     = dt.utcnow()#4 fecha inicio del proceso 
  url2      ='https://restcountries.eu/rest/v2/region/'+region
  response  = requests.request("GET", url2)
  respuesta = response.json()[0]
  pais      = respuesta['name']
  idioma    = respuesta['languages'][0]['name']
  h         = hashlib.new("sha1",bytes(idioma, encoding='utf-8'))
  date2     = dt.utcnow() #4
  tiempo    = (date2-date1) #4 tiempo total del proceso
  if logs:
    logging.debug('region: {0},pais:{1},idioma:{2},SHA1:{3},tiempo{4} '.format(region,pais,idioma,h.hexdigest(),tiempo.total_seconds()))
  return region,pais,idioma,h.hexdigest(),tiempo.total_seconds()

#5 La tabla debe ser creada en un DataFrame con la libreria PANDAS
def creaDataFrame(regiones):
  """ Se crea la tabla con pandas
  >>> regiones=  {'', 'Polar', 'Africa', 'Europe', 'Asia', 'Americas', 'Oceania'}
  >>> datos=creaDataFrame(regiones)
  >>> Ciudades=len(datos['City Name'].to_list())==6
  >>> print('Hay 6 ciudades', Ciudades)
  Hay 6 ciudades True
  """
  df=pd.DataFrame()
  for region in regiones: 
    if region:
      regionData,pais,idioma,h,tiempo = regionPaisIdioma(region)
      d={"Region":regionData,"City Name":pais,"Languaje":h,"Time":tiempo}
      df=df.append(d , ignore_index=True)
  if logs:
    logging.debug('Tabla Pandas: \n {}'.format(df))
  return df

#6  Con funciones de la libreria pandas muestre el tiempo total, el tiempo promedio, el tiempo minimo y el maximo que tardo en procesar toda las filas de la tabla.

def funcionesPandas(datos):   
    """
    Funciones basicas pandas esta información tambien se encuentra en la funcion describe() de pandas.
    >>> d  = {'Time': [4, 5, 6]}
    >>> df = pd.DataFrame(data=d)
    >>> funcionesPandas(df)
    'TiempoTotal:15 TiempoPromedio:5.0 TiempoMinimo:4 TiempoMaximo:6'
    """
    tiempoTotal=datos['Time'].sum()
    tiempoPromedio=datos['Time'].mean()
    tiempoMinimo=datos['Time'].min()
    tiempoMaximo=datos['Time'].max()
    #print('TiempoTotal:{0} TiempoPromedio:{1} TiempoMinimo:{2} TiempoMaximo:{3}'.format(tiempoTotal, tiempoPromedio, tiempoMinimo, tiempoMaximo))
    respuesta = 'TiempoTotal:{0} TiempoPromedio:{1} TiempoMinimo:{2} TiempoMaximo:{3}'.format(tiempoTotal, tiempoPromedio, tiempoMinimo, tiempoMaximo)
    if logs:
      logging.debug('funciones pandas: {}'.format(respuesta))
    return respuesta

#7. Guarde el resultado en sqlite.
#8. Genere un Json de la tabla creada y guardelo como data.json 
def resultadoSqlLite(datos,rutajson):
  """ dataframe a SqlLite y SqlLite to Json
  """
  if path.exists('base.db'):
    remove('base.db')
  #1 guardar en tabla sqlLite
  conn = sqlite3.connect('base.db')
  datos.to_sql('pythonChallenge', conn)
  #2 consultar la tabla del paso anterior
  respuesta = pd.read_sql('SELECT * FROM pythonChallenge', conn)
  #3 guardar en Json
  respuesta.to_json(rutajson)
  if logs:
    logging.debug('crea Json: {}'.format(rutajson))
  return path.exists(rutajson)



if __name__ == "__main__":
  
    regiones = extraeRegiones()
    datos=creaDataFrame(regiones)
    resultadoSqlLite(datos,'archivo.json')
    if unitTest:
      import doctest
      doctest.testmod(verbose=True)