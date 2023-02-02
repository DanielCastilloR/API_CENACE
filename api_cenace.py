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

"""# **Clases**"""

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
  
  def generar_url(self):
    self.url = 'https://ws01.cenace.gob.mx:8082/SWPML/SIM/' + self.sistema + '/' + self.proceso + '/' + self.nodo + '/' + self.ano_ini + '/' + self.mes_ini + '/' + self.dia_ini + '/' + self.ano_fin + '/' + self.mes_fin + '/' + self.dia_fin + '/' + self.formato

  def generar_DF(self):
    self.generar_url()
    try:
      response = urlopen(self.url)
      data_json = json.loads(response.read())
      self.status = data_json["status"]
      data_json = data_json["Resultados"]
      df = pd.json_normalize(data_json,record_path =['Valores'])
      return df
    except:
        print('Bad request en fecha:'+ self.ano_ini + '/' + self.mes_ini + '/' + self.dia_ini + ' a ' + self.ano_fin + '/' + self.mes_fin + '/' + self.dia_fin)

class Mapas():
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
        CENACE = API(sistema,proceso,nodo,str(dia1.year),keys.get(str(dia1.month),str(dia1.month)),keys.get(str(dia1.day),str(dia1.day)),str(dia2.year),keys.get(str(dia2.month),str(dia2.month)),keys.get(str(dia2.day),str(dia2.day)),formato)
        try:
          CENACE.generar_DF()
          if CENACE.status != 'ZERO_RESULTS':
            self.Precios = pd.concat([self.Precios,CENACE.generar_DF()])
        except:
          print('Cero resultados en fecha: ')
    else:
      CENACE = API(self.sistema,self.proceso,self.nodo,self.ano_ini,self.mes_ini,self.dia_ini,self.ano_fin,self.mes_fin,self.dia_fin,self.formato)
      self.Precios = CENACE.generar_DF()
    if self.Precios.shape != (0,0):
      self.Precios = self.Precios.reset_index(drop=True)
      self.Precios.iloc[:,1:] = self.Precios.iloc[:,1:].astype('float')
      self.Precios.reset_index(drop=True)
      self.Precios = self.Precios.drop_duplicates(subset=['fecha','hora'])
      meses = []
      anos = []
      for i in range(len(self.Precios)):
        meses.append(int(self.Precios.iloc[i,0][5:7]))
        anos.append(int(self.Precios.iloc[i,0][:4]))
      self.Precios['Mes'] = meses
      self.Precios['Año'] = anos
      self.Precios['hora'] = self.Precios['hora'].replace(25.0,24.0)
      self.Precios2 = self.Precios.groupby(['Año','Mes','hora']).mean()
      Anos = []
      Meses = []
      Horas = []
      for i in range(len(self.Precios2)):
        Anos.append(self.Precios2.index[i][0])
        Meses.append(self.Precios2.index[i][1])
        Horas.append(self.Precios2.index[i][2])
      Anos = pd.DataFrame(Anos)
      Meses = pd.DataFrame(Meses)
      Horas = pd.DataFrame(Horas)
      self.PML_prom = pd.DataFrame((self.Precios2.values),columns=['PML','PML_ene','PML_per','PML_cng'])
      self.PML_prom.insert(0,'Año',Anos)
      self.PML_prom.insert(1,'Mes',Meses)
      self.PML_prom.insert(2,'Hora',Horas)
      Anos = np.unique(Anos)
      Meses = np.unique(Meses)
      keys = {1:'Enero',2:'Febrero',3:'Marzo',4:'Abril',5:'Mayo',6:'Junio',7:'Julio',8:'Agosto',9:'Septiembre',10:'Octubre',11:'Noviembre',12:'Diciembre'}
      MESES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']


      #PML
      for i in Anos:
        self.PML_ANO = pd.DataFrame()
        PML = self.PML_prom[self.PML_prom['Año']==i] 
        for j in Meses:
          PML2 = PML[PML['Mes']==j]
          PML2 = PML2.drop(['Año','Mes','Hora'],axis=1)
          PML2= PML2['PML']
          self.PML_ANO = pd.concat([self.PML_ANO.reset_index(drop=True),PML2.reset_index(drop=True)],axis=1)
        self.PML_ANO.columns = MESES
        self.PML_ANO.index = (np.arange(1,len(self.PML_ANO)+1))
        self.plot_heatmap(i,'PML')
        self.agregar_gsheet('PML',i)

      #PML Energia
      for i in Anos:
        self.PML_ANO = pd.DataFrame()
        PML = self.PML_prom[self.PML_prom['Año']==i] 
        for j in Meses:
          PML2 = PML[PML['Mes']==j]
          PML2 = PML2.drop(['Año','Mes','Hora'],axis=1)
          PML2= PML2['PML_ene']
          self.PML_ANO = pd.concat([self.PML_ANO.reset_index(drop=True),PML2.reset_index(drop=True)],axis=1)
        self.PML_ANO.columns = MESES
        self.PML_ANO.index = (np.arange(1,len(self.PML_ANO)+1))
        self.plot_heatmap(i,'PML_Energia')
        self.agregar_gsheet('PML ENERGIA',i)

      #PML Perdida
      for i in Anos:
        self.PML_ANO = pd.DataFrame()
        PML = self.PML_prom[self.PML_prom['Año']==i] 
        for j in Meses:
          PML2 = PML[PML['Mes']==j]
          PML2 = PML2.drop(['Año','Mes','Hora'],axis=1)
          PML2= PML2['PML_per']
          self.PML_ANO = pd.concat([self.PML_ANO.reset_index(drop=True),PML2.reset_index(drop=True)],axis=1)
        self.PML_ANO.columns = MESES
        self.PML_ANO.index = (np.arange(1,len(self.PML_ANO)+1))
        self.plot_heatmap(i,'PML_Perdida')
        self.agregar_gsheet('PML PERDIDA',i)

      #PML Congestión
      for i in Anos:
        self.PML_ANO = pd.DataFrame()
        PML = self.PML_prom[self.PML_prom['Año']==i] 
        for j in Meses:
          PML2 = PML[PML['Mes']==j]
          PML2 = PML2.drop(['Año','Mes','Hora'],axis=1)
          PML2= PML2['PML_cng']
          self.PML_ANO = pd.concat([self.PML_ANO.reset_index(drop=True),PML2.reset_index(drop=True)],axis=1)
        self.PML_ANO.columns = MESES
        self.PML_ANO.index = (np.arange(1,len(self.PML_ANO)+1))
        self.plot_heatmap(i,'PML_Congestión')
    else:
      print('Sin resultados para el nodo: '+self.nodo+" En la fecha de: "+ self.ano_ini + '/' + self.mes_ini + '/' + self.dia_ini + ' a ' + self.ano_fin + '/' + self.mes_fin + '/' + self.dia_fin)

  def plot_heatmap(self,i,tipo):
    keys = {1:'Enero',2:'Febrero',3:'Marzo',4:'Abril',5:'Mayo',6:'Junio',7:'Julio',8:'Agosto',9:'Septiembre',10:'Octubre',11:'Noviembre',12:'Diciembre'}
    rdgn = sns.diverging_palette(h_neg=130, h_pos=10, s=99, l=55, sep=3, as_cmap=True)
    fig, axes = plt.subplots(figsize=(20,20))
    sns.heatmap(self.PML_ANO,annot=True,fmt=".2f",cmap=rdgn,cbar=False ,ax=axes)
    plt.ylabel('Horas')
    #plt.xlabel('Precios')
    plt.suptitle('Análisis Nodal {}-{}'.format(self.nodo,tipo),size=25)
    plt.title('Nodo: {}     Año: {}   Unidades: mxn$/MWh    MPL     Min: {}   Max: {}     {}'.format(self.nodo, i , np.round(min(self.PML_ANO.values[~np.isnan(self.PML_ANO.values)]),2) , np.round(max(self.PML_ANO.values[~np.isnan(self.PML_ANO.values)]),2) , self.proceso), size=14)
    plt.savefig('/content/gdrive/Shareddrives/Electricidad/API CENACE/Mapas/{}_{}_{}_{}.jpg'.format(self.nodo,i,self.proceso,tipo))
    plt.show()



"""# **Precios**"""

sistema = 'SIN'
proceso = 'MDA'
nodo = '02PUE-115'  #04MMU-115  02PUE-115  01/12/2016   --> PRUEBA 01/01/2016
# Fecha de Inicio
ano_ini = '2018'
mes_ini = '01'
dia_ini = '01'
# Fecha de Fin
ano_fin = '2019'
mes_fin = '12'
dia_fin = '31'

formato = 'JSON'

CENACE = Mapas(sistema,proceso,nodo,ano_ini,mes_ini,dia_ini,ano_fin,mes_fin,dia_fin,formato)

CENACE.obtener_precios()
