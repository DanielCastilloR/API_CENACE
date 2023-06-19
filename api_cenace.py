# -*- coding: utf-8 -*-
"""
Created on Sat Jan 28 17:15:19 2023

@author: Daniel Castillo
"""

import pandas as pd
import numpy as np
import sys
import time
import itertools
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
import json
from urllib.request import urlopen
import datetime as dt
import seaborn as sns



class API:
  def __init__(self,sistema,proceso,nodo,ano_ini,mes_ini,dia_ini,ano_fin,mes_fin,dia_fin,formato):
    self.sistema = sistema
    self.proceso = proceso
    self.nodo = nodo
    self.ano_ini = ano_ini
    self.mes_ini = mes_ini
    self.dia_ini = dia_ini
    self.ano_fin = ano_fin
    self.mes_fin = mes_fin
    self.dia_fin = dia_fin
    self.formato = formato
    
  
  def generar_DF(self,ano_ini1,mes_ini1,dia_ini1,ano_fin1,mes_fin1,dia_fin1):
    url = 'https://ws01.cenace.gob.mx:8082/SWPML/SIM/' + self.sistema + '/' + self.proceso + '/' + self.nodo + '/' + ano_ini1 + '/' + mes_ini1 + '/' + dia_ini1 + '/' + ano_fin1 + '/' + mes_fin1 + '/' + dia_fin1 + '/' + self.formato
    try:
      response = urlopen(url)
      data_json = json.loads(response.read())
      self.status = data_json["status"]
      data_json = data_json["Resultados"]
      df = pd.json_normalize(data_json,record_path =['Valores'])
      return df
    except:
        print('Bad request en fecha:'+ ano_ini1 + '/' + mes_ini1 + '/' + dia_ini1 + ' a ' + ano_fin1 + '/' + mes_fin1 + '/' + dia_fin1)


  def split(self,our_list): 
    chunked_list = list()
    chunk_size = 6
    for i in range(0, len(our_list), chunk_size):
        chunked_list.append(our_list[i:i+chunk_size])
    return(chunked_list)

  def obtener_precios(self):
    sep =  dt.date(int(self.ano_fin),int(self.mes_fin),int(self.dia_fin)) - dt.date(int(self.ano_ini),int(self.mes_ini),int(self.dia_ini))
    if sep.days >=7: 
      dias_tomar = self.split(np.arange(1,sep.days+1))
      keys = {'1':'01','2':'02','3':'03','4':'04','5':'05','6':'06','7':'07','8':'08','9':'09'}
      self.Precios = pd.DataFrame()
      dia1 = dt.date(int(self.ano_ini),int(self.mes_ini),int(self.dia_ini)) 
      dia2 = dia1
      for i in range(len(dias_tomar)):
        fin = len(dias_tomar[i]) 
        dia1 = dia2
        dia2 = dia2 +  dt.timedelta(days=int(fin))
        try:
          CENACE = self.generar_DF(str(dia1.year),keys.get(str(dia1.month),str(dia1.month)),keys.get(str(dia1.day),str(dia1.day)),str(dia2.year),keys.get(str(dia2.month),str(dia2.month)),keys.get(str(dia2.day),str(dia2.day)))
          if self.status != 'ZERO_RESULTS':
            self.Precios = pd.concat([self.Precios,CENACE])
        except:
          print('Cero resultados en fecha: ')
    else:
      self.Precios = self.generar_DF(self.ano_ini,self.mes_ini,self.dia_ini,self.ano_fin,self.mes_fin,self.dia_fin)
    if self.Precios.shape != (0,0):
      self.Precios = self.Precios.reset_index(drop=True)
      self.Precios.iloc[:,1:] = self.Precios.iloc[:,1:].astype('float')
      self.Precios.reset_index(drop=True)
      self.Precios = self.Precios.drop_duplicates(subset=['fecha','hora'])
     
    else:
      print('Sin resultados para el nodo: '+self.nodo+" En la fecha de: "+ self.ano_ini + '/' + self.mes_ini + '/' + self.dia_ini + ' a ' + self.ano_fin + '/' + self.mes_fin + '/' + self.dia_fin)



sistema = 'BCA'
proceso = 'MTR'
nodo = '07CRO-161'  #04MMU-115  02PUE-115  01/12/2016   --> PRUEBA 01/01/201607CPT-230
# Fecha de Inicio
ano_ini = '2020'
mes_ini = '01'
dia_ini = '01'
# Fecha de Fin
ano_fin = '2021'
mes_fin = '12'
dia_fin = '31'
formato = 'JSON'

data = API(sistema,proceso,nodo,ano_ini,mes_ini,dia_ini,ano_fin,mes_fin,dia_fin,formato)
data.obtener_precios()
datos = data.Precios
