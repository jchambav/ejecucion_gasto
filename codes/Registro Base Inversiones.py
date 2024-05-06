#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importar paquetes
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sys
sys.path
sys.path.append('C:/Users/jchamba/Documents/Jannely/Proyecto12/unicode-2.9')
from unidecode import unidecode

from IPython.display import display, HTML
display(HTML("<style>.container { width:90% !important; }</style>"))


# In[2]:


#Importamos variables temporales
now = datetime.now()
ayer = datetime.now() - timedelta(days=1)
anteayer = datetime.now() - timedelta(days=2)

# la fecha de corte es la más actual
fecha_corte = now.strftime("%d") + now.strftime("%m") + now.strftime("%Y") # es la fecha actual en formato dd/mm/yyyy
print(now.strftime("%d") + '-' + now.strftime("%m") + '-' + now.strftime("%Y"))


# In[3]:


# Para limpiar variables duplicadas luego de un merge
def postmerge(df):
    replace_list = [i for i in df.columns if (i.endswith('_x'))]
    replace_list = [i[:-2] for i in replace_list]
    for i in replace_list:
        df[i] = df[i+'_x'].fillna(df[i+'_y'])
    df.drop([i for i in df.columns if (i.endswith('_x'))|(i.endswith('_y'))], inplace=True, axis=1)
    return df

# Para crear el COD_ID de la ejecutora
def crear_id(df, NIVEL_GOB, SECTOR, PLIEGO, EJECUTORA):
    df = df.assign(COD_ID = np.where(df[NIVEL_GOB].str.startswith('2'), (df[PLIEGO].str.split('. ', n=1).str[0] +"-" + df[EJECUTORA].str.split('. ', n=1).str[0]),
                                     (df[SECTOR].str.split('. ', n=1).str[0] +"-" + df[PLIEGO].str.split('. ', n=1).str[0] +"-"+ df[EJECUTORA].str.split('. ', n=1).str[0])))
    return df


# In[5]:


### Cargar bases
print(datetime.now())
bd_ejecutoras = pd.read_csv(os.path.join(r'Y:\Data_CSV\BancoInv\EjecutorasPptal', 'Ejecutoras_CUI_25042024'+'.csv'),dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_secejec = pd.read_excel(os.path.join(r'Y:\Data_CSV\Miscelanea', 'SEC_EJEC_UBIGEO' + '.xlsx'), dtype={'ID_PLIEGO': str, 'CODIGOSIAF': str, 'SEC_EJEC': str})
bd_dictinv = pd.read_csv(os.path.join(r'Y:\Data_CSV\BancoInv\NombreProyecto', 'Invierte_Nombres_' + fecha_corte +'.csv'),dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_invierte = pd.read_csv(os.path.join(r'Y:\Data_CSV\BancoInv', 'InviertePe_'+ fecha_corte + '.csv'),dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_seg24 = pd.read_csv(os.path.join(r'Y:\Data_CSV\SIAFGasto2024', 'SeguimientoGasto_2024_'+ fecha_corte +'.csv'),dtype={'COD_PRODUCTO_PROYECTO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_seg23 = pd.read_csv(os.path.join(r'Y:\Data_CSV\SIAFGasto2023', 'SeguimientoGasto_2023_12042024'+'.csv'),dtype={'COD_PRODUCTO_PROYECTO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_dictseg24= pd.read_excel(os.path.join(r'Y:\Data_CSV\SIAFGasto2024', 'diccionarios_seguimiento_2024' + '.xlsx'), dtype={'COD_PRODUCTO_PROYECTO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_dictseg23= pd.read_excel(os.path.join(r'Y:\Data_CSV\SIAFGasto2023', 'diccionarios_seguimiento_2023' + '.xlsx'), dtype={'COD_PRODUCTO_PROYECTO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_listas= pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 15', 'LISTAS_DGPP' + '.xlsx'), dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_dispositivos = pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 15', 'Reporte_29042024' + '.xlsx'), sheet_name='DISPOSITIVOS', dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_origen = pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 15', 'Reporte_29042024' + '.xlsx'), sheet_name='LISTA', dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
bd_ldapt = pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 15', 'Reporte_29042024' + '.xlsx'), sheet_name='ENVIOS_DAPT_SECTOR', dtype={'CODIGO_UNICO': str, 'COD_ID': str, 'SEC_EJEC': str})
print(datetime.now())


# In[6]:


listas = bd_listas.copy()
bd_secejec.info()
bd_secejec.head()


# In[7]:


origen = bd_origen.copy()
origen['ORIGEN'] = origen['ORIGEN'].replace({'Sectores':'Demandas enviadas por Sectores', 
                                             'Lista DAPT':'Demandas recibidas por DAPT',
                                             'PCM':'Lista de 94 inversiones de PCM',
                                             'Punche 2':'Anexo 03 Punche', 
                                             'LISTA IRI':'Lista de IRI (Reconstrucción)',
                                             'Lista Alcaldesas':'Lista enviada por Alcaldesas',
                                             'CONGRESO' : 'Lista Congreso',
                                             'Ficha 5': 'Ficha 5 (PMG 2024-2026)'}) 
origen = origen.groupby(['CODIGO_UNICO', 'ORIGEN']).agg({'EJEC_2027': 'sum'}).reset_index()
origen = origen.groupby(['CODIGO_UNICO']).agg({'ORIGEN':' // '.join}).reset_index()
origen.info()
origen.head()


# In[8]:


dispositivos = bd_dispositivos.copy()
dispositivos.rename(columns={'MARCO_TRANSFERIDO':'MARCO_2024',
                             'DISPOSITIVO':'DISPOSITIVO_2024'}, inplace=True)
dispositivos = dispositivos.groupby(['CODIGO_UNICO', 'DISPOSITIVO_2024']).agg({'MARCO_2024':'sum'}).reset_index()
dispositivos = dispositivos.groupby(['CODIGO_UNICO']).agg({'MARCO_2024':'sum', 'DISPOSITIVO_2024':' // '.join}).reset_index()
dispositivos = pd.merge(dispositivos, bd_invierte[['CODIGO_UNICO', 'TIPO_PROYECTO']].drop_duplicates(), on=['CODIGO_UNICO'], how='left')
dispositivos = dispositivos[dispositivos['TIPO_PROYECTO'] != 'GENERICO'].reset_index(drop=True)
dispositivos.drop(columns=['TIPO_PROYECTO'], inplace=True)
postmerge(dispositivos)
dispositivos.info()
dispositivos.head()
dispositivos['MARCO_2024'].sum()


# In[13]:


listas_dapt= bd_ldapt.copy()
listas_dapt.info()
listas_dapt.head()


# In[14]:


lis_dis = pd.merge(listas, origen, on=['CODIGO_UNICO'], how='outer')
postmerge(lis_dis)
lis_dis = pd.merge(lis_dis, dispositivos, on=['CODIGO_UNICO'], how='outer')
postmerge(lis_dis)
lis_dis['Validación_Listas']= np.where(lis_dis['LISTAS']==lis_dis['ORIGEN'], 'SI', 'NO')
lis_dis.info()
lis_dis.head()


# In[15]:


invierte = bd_invierte.copy()
invierte.drop_duplicates(inplace=True)
invierte = invierte[['CODIGO_UNICO', 'ESTADO', 'TIPO_PROYECTO', 'EXP_TCO', 'FUNCION', 'COSTO_ACTUAL_BCO', 'DEV_ACUM_AL2023']]
invierte['ESTADO'] = invierte['ESTADO'].replace({'A': 'Activo',
                                                 'C':'Cerrado',
                                                 'D': 'Desactivado',
                                                 'Z':'Fuera de Invierte'})

invierte['TIPO_PROYECTO'] = invierte['TIPO_PROYECTO'].replace({'PI-INVIERTE':'Proyecto de inversión - Invierte.pe', 
                                                                 'PI-SNIP':'Proyecto de inversión - SNIP',
                                                                 'IOARR':'IOARR',
                                                                 'FUR/IRI':'Intervención de reconstrucción mediante inversiones (IRI)'}) 

invierte.drop_duplicates(inplace=True)
invierte.info()
invierte.head()


# In[16]:


ejecutoras = bd_ejecutoras.copy()
secejec = bd_secejec.copy()
Base = pd.merge(invierte, ejecutoras[['CODIGO_UNICO', 'COD_ID','SEC_EJEC']], on=['CODIGO_UNICO'], how='left')
postmerge(Base)
Base = pd.merge(Base, secejec[['SEC_EJEC', 'PLIEGO_SIAF_RES', 'DEPARTAMENTO']], on=['SEC_EJEC'], how='left')
postmerge(Base)
Base = pd.merge(Base, bd_dictinv[['CODIGO_UNICO', 'NOMBRE_PROYECTO']], on=['CODIGO_UNICO'], how='left')
postmerge(Base)
Base['PLIEGO_SIAF_RES'] = np.where(Base['PLIEGO_SIAF_RES'].isna(), "SIN IDENTIFICAR", Base['PLIEGO_SIAF_RES'])
Base.info()
Base.head(500585)


# In[17]:


bd_seg24.info()
bd_seg24.head()


# In[18]:


seg23 = bd_seg23.copy()
seg23.rename(columns={'COD_PRODUCTO_PROYECTO':'CODIGO_UNICO',
                      'PIM':'PIM_2023',
                      'TOTAL_DEVENGADO' : 'DEV_2023'}, inplace=True)
seg23 = seg23.groupby(['CODIGO_UNICO']).agg({'PIM_2023':'sum', 'DEV_2023':'sum'}).reset_index()
seg23['CODIGO_UNICO'] = seg23['CODIGO_UNICO'].astype(str)
seg23['DEV_2023'] = np.round(seg23['DEV_2023'],0)
seg23['DEV_2023'] = seg23['DEV_2023'].astype(int)
seg23['PIM_2023'] = seg23['PIM_2023'].round()
seg23['%Ejec2023'] = (seg23['DEV_2023'].fillna(0)/seg23['PIM_2023'].fillna(0))
seg23.info()
seg23.head(46471)


# In[19]:


seg24 = bd_seg24.copy()
seg24.rename(columns={'COD_PRODUCTO_PROYECTO':'CODIGO_UNICO',
                      'PIM':'PIM_2024',
                      'CERTIFICADO':'CERTIFICADO_2024',
                      'COMPROMISO_ANUAL':'COMPROMISO_2024',
                      'TOTAL_DEVENGADO' : 'DEV_2024'}, inplace=True)
seg24 = seg24.groupby(['CODIGO_UNICO']).agg({'PIM_2024':'sum', 'CERTIFICADO_2024':'sum', 'COMPROMISO_2024':'sum', 'DEV_2024':'sum'}).reset_index()
seg24['DEV_2024'] = np.round(seg24['DEV_2024'],0)
seg24['DEV_2024'] = seg24['DEV_2024'].astype(int)
seg24['PIM_2024'] = seg24['PIM_2024'].round()
seg24['%Ejec2024'] = (seg24['DEV_2024'].fillna(0)/seg24['PIM_2024'].fillna(0))
seg24['CODIGO_UNICO'] = seg24['CODIGO_UNICO'].astype(str)

seg24.info()
seg24.head(46471)


# In[20]:


Base1 = pd.merge(Base, seg24, on=['CODIGO_UNICO'], how='left')
postmerge(Base1)
Base1 = pd.merge(Base1, seg23, on=['CODIGO_UNICO'], how='left')
postmerge(Base1)
Base2 = pd.merge(Base1, lis_dis, on=['CODIGO_UNICO'], how='left')
postmerge(Base2)
Base2 = pd.merge(Base2, listas_dapt, on=['CODIGO_UNICO'], how='left')
postmerge(Base2)
Base2.rename(columns={'PLIEGO_SIAF_RES':'PLIEGO',
                      'DEV_ACUM_AL2023': 'AVANCE_EJEC_2023'}, inplace=True)
Base2['LISTAS'] = np.where(Base2['LISTAS'].isna(), "NO SE ENCUENTRA", Base2['LISTAS'])
Base2['PENDIENTE_FINANCIAR'] = Base2 ['COSTO_ACTUAL_BCO'].fillna(0) - Base2 ['AVANCE_EJEC_2023'].fillna(0)- Base2 ['PIM_2024'].fillna(0)
Base2['PENDIENTE_FINANCIAR'] = np.where(Base2['PENDIENTE_FINANCIAR'].fillna(0)<=0, 0,
                                        np.where(Base2['TIPO_PROYECTO']=='GENERICO', 0, Base2['PENDIENTE_FINANCIAR']))
Base2['%AvanceCierre2023'] = (Base2['AVANCE_EJEC_2023'].fillna(0)/Base2['COSTO_ACTUAL_BCO'].fillna(0))
Base2['%AvanceCierre2023'] = np.where(Base2['%AvanceCierre2023'].fillna(0)<=0, 0,
                                        np.where(np.isinf(Base2['%AvanceCierre2023']), ' ', Base2['%AvanceCierre2023']))
Base2['%Demanda enviada a sectores'] = np.where(Base2['LISTA_DAPT']=='Si', 'SI', 'NO')
Base2 = Base2[['CODIGO_UNICO', 'NOMBRE_PROYECTO', 'PLIEGO', 'DEPARTAMENTO', 'FUNCION', 'ESTADO', 'TIPO_PROYECTO', 'EXP_TCO', 'COSTO_ACTUAL_BCO', 'AVANCE_EJEC_2023', '%AvanceCierre2023', 'PIM_2023', 'DEV_2023', '%Ejec2023', 'PIM_2024', 'CERTIFICADO_2024', 'COMPROMISO_2024', 'DEV_2024', '%Ejec2024', 'PENDIENTE_FINANCIAR', 'ORIGEN', 'DISPOSITIVO_2024',  'MARCO_2024', '%Demanda enviada a sectores', 'HR']]
Base2.rename(columns={'ORIGEN':'LISTAS'}, inplace=True)
Base2.info()
Base2.head()


# In[21]:


#%%
outputFile = os.path.join(r'Y:\Registros\Base_Inversiones', 'Base_CUI_' +  fecha_corte + ".xlsx")
with pd.ExcelWriter(outputFile) as ew:
    Base2.to_excel(ew, sheet_name="Base_Inversiones", index = False)   



