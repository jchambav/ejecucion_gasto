#!/usr/bin/env python
# coding: utf-8

# # <span style="color:red"> NOTEBOOK PARA EL ANÁLISIS DE LAS NOTAS MODIFICATORIAS </span>

# #### El tablero va a mostrar los cambios en el presupuesto de las partidas (productos y proyectos) a diferentes niveles. Se incluyen tanto las notas de tipo 3 y 4 como modificaciones en el nivel institucional

# Primero importamos los módulos y paquetes

# In[1]:


import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from IPython.display import display, HTML
display(HTML("<style>.container { width:90% !important; }</style>"))


# In[ ]:





# Definimos variables temporales para que trabajemos con las bases más actuales

# In[2]:


now = datetime.now()
ayer = datetime.now() - timedelta(days=1)
anteayer = datetime.now() - timedelta(days=2)

# la fecha de corte es la más actual
fecha_corte = now.strftime("%d") + now.strftime("%m") + now.strftime("%Y") # es la fecha actual en formato dd/mm/yyyy ---- now.strftime("%d")
print(now.strftime("%d") + '-' + now.strftime("%m") + '-' + now.strftime("%Y"), fecha_corte)


# Definimos algunas funciones para facilitar procesos

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


# Ahora procedemos con la carga de información.
# En principio es necesario contar con: 
# 
# 0. Información de la cadena institucional (Sector, pliego, ejecutora y sec_ejec)
# 1. Información de las notas modificatorias (Nivel requerido: ID, CUI, ACT, PP, FF, RR, CAT, GEN, MES, PLANT, TIPO, DISPO, INGR, CREDANULA)
# 2. Información presupuestaria del SIAF (Nivel requerido: ID, CUI, ACT, PP, FF, RR, CAT, GEN)
# 3. Información del banco de inversiones (Nivel requerido: ID, CUI)
# 4. Información sobre los dispositivos legales que modifican marco presupuestal (Nivel requerido: ID, CUI, ACT, PP, FF, RR, CAT, GEN, DISPO, CREDANULA)
# 5. Datos de los anexos de la Ley
# 

# ## <span style="color:green"> PRIMERA SECCIÓN: Carga de bases de datos </span>

# ### <span style="color:blue"> 0. Información de la cadena institucional </span>

# In[4]:


bsec_ejec = pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\BASES', 'SEC_EJEC_UBIGEO' + ".xlsx"), dtype={'UBIGEO':str})
bsec_ejec[bsec_ejec['NIVEL_GOB'].str[0] == '3'].head()


# ### <span style="color:blue"> 1. Información de las notas modificatorias </span>
# 
# En el caso de las notas modificatorias, estas son cargadas desde 3 diferentes archivos, cada uno correspondiente a un nivel de gobierno. Asimismo, los reportes de ECI pueden contener varias hojas, las cuales se generan según el semestre del año o según la cantidad de filas que ya han sido ocupadas en cada hoja (llegan a su límite por lo que se pasa la información a otra hoja). 
# 
# Por ello se hace un proceso iterativo para lograr acumular toda la información disponible

# In[5]:


# 1. Información de las notas modificatorias
ruta = r'Z:\Seguimiento del Gasto\Notas Modificatorias\2024'

# Gobierno Nacional
if os.path.isfile(os.path.join(ruta, '1.NotasModif_I_II_Sem2024_GN_' + fecha_corte + '.xlsx')) is True:
    # definimos el df para completar
    bd_gn = pd.DataFrame()
    # verificamos los sheets del archivo
    sheets = pd.ExcelFile(os.path.join(ruta, '1.NotasModif_I_II_Sem2024_GN_' + fecha_corte + '.xlsx'))
    
    for n in sheets.sheet_names:
        bd_notas = pd.read_excel(os.path.join(ruta, '1.NotasModif_I_II_Sem2024_GN_' + fecha_corte + '.xlsx'), sheet_name=n,
                                    usecols='F, G, H, I, L, M, Q, T, U, W, X, Y, AE, AF, AI, AJ, AK, AL, AO, AQ, AR')
        bd_gn = pd.concat([bd_gn, bd_notas], ignore_index=True)

# Gobierno Regional
if os.path.isfile(os.path.join(ruta, '2.NotasModif_I_II_Sem2024_GR_' + fecha_corte + '.xlsx')) is True:
    # definimos el df para completar
    bd_gr = pd.DataFrame()
    # verificamos los sheets del archivo
    sheets = pd.ExcelFile(os.path.join(ruta, '2.NotasModif_I_II_Sem2024_GR_' + fecha_corte + '.xlsx'))
    
    for n in sheets.sheet_names:
        bd_notas = pd.read_excel(os.path.join(ruta, '2.NotasModif_I_II_Sem2024_GR_' + fecha_corte + '.xlsx'), sheet_name=n,
                                    usecols='F, G, H, I, L, M, Q, T, U, W, X, Y, AE, AF, AI, AJ, AK, AL, AO, AQ, AR')
        bd_gr = pd.concat([bd_gr, bd_notas], ignore_index=True)    

# Gobierno Local
if os.path.isfile(os.path.join(ruta, '3.NotasModif_I_II_Sem2024_GL_' + fecha_corte + '.xlsx')) is True:
    # definimos el df para completar
    bd_gl = pd.DataFrame()
    # verificamos los sheets del archivo
    sheets = pd.ExcelFile(os.path.join(ruta, '3.NotasModif_I_II_Sem2024_GL_' + fecha_corte + '.xlsx'))
    
    for n in sheets.sheet_names:
        bd_notas = pd.read_excel(os.path.join(ruta, '3.NotasModif_I_II_Sem2024_GL_' + fecha_corte + '.xlsx'), sheet_name=n,
                                    usecols='F, G, H, I, L, M, Q, T, U, W, X, Y, AE, AF, AI, AJ, AK, AL, AO, AQ, AR')
        bd_gl = pd.concat([bd_gl, bd_notas], ignore_index=True)   


# ### <span style="color:blue"> 2. Información presupuestaria del SIAF </span>
# 
# La información presupuestaria se obtiene de las bases (reportes) del SIAF que genera ECI. Estos tienen información del PIA, PIM, Certificación, Compromisos así como devengados, los cuales son de nuestro interés para hacer validaciones y tener una comparativa del antes y después. Al igual que las bases de modificaciones, esta información está separada en archivos por nivel de gobierno; sin embargo, en cada archivo la información se consolida en una sola hoja.

# In[6]:


ruta2 = r'Z:\Seguimiento del Gasto\Informacion Quincenal\2024'
# niveles de anàlisis: producto, actividad, pp, fuente, rubro, generica
# Desempeño presupuestario 2024
if os.path.isfile(os.path.join(ruta2, '1.PIAPIMDevGirxMetaEsp_2024_GN_' + fecha_corte + '.xlsx')) is True:
    # revisamos los indicadores presupuestarios
    bd_gn_seg = pd.read_excel(os.path.join(ruta2, '1.PIAPIMDevGirxMetaEsp_2024_GN_' + fecha_corte + '.xlsx'), sheet_name='GN', usecols='F, H, J, K, P, S, T, U, V, W, X, Y, AG, AH, AI, AJ, AW') #AG, AH, AI, AJ, AW
    bd_gr_seg = pd.read_excel(os.path.join(ruta2, '2.PIAPIMDevGirxMetaEsp_2024_GR_' + fecha_corte + '.xlsx'), sheet_name='GR', usecols='F, H, J, K, P, S, T, U, V, W, X, Y, AG, AH, AI, AJ, AW')
    bd_gl_seg = pd.read_excel(os.path.join(ruta2, '3.PIAPIMDevGirxMetaEsp_2024_GL_' + fecha_corte + '.xlsx'), sheet_name='GL', usecols='F, H, J, K, P, S, T, U, V, W, X, Y, AG, AH, AI, AJ, AW')    


# In[7]:


bd_gn_seg.columns
bd_gn_seg.shape


# In[8]:


print('GN: S/', '{:,.0f}'.format(bd_gn_seg.PIM.sum()), ', nro_obs= ', bd_gn_seg.shape[0])
print('GR: S/', '{:,.0f}'.format(bd_gr_seg.PIM.sum()), ', nro_obs= ', bd_gr_seg.shape[0])
print('GL: S/', '{:,.0f}'.format(bd_gl_seg.PIM.sum()), ', nro_obs= ', bd_gl_seg.shape[0])


# ### <span style="color:blue"> 3. Información sobre los dispositivos legales que modifican marco presupuesta </span>
# 
# Información de dispositivos legales que han sido aprobados en el año, tales como créditos suplementarios, decretos supremos para incorporarción o reducción de marco, leyes, etc.

# In[9]:


bd_dispositivos = pd.read_excel(os.path.join(r'Z:\Seguimiento del Gasto\Seguimiento por Dispositivo\2024', 'Dispositivos_Legales_2024_' + fecha_corte + '.xlsx'))


# ### <span style="color:blue"> 4. Datos de los anexos de la Ley </span>
# 
# Otras fuentes de información para estructurar bien la base de datos final

# In[10]:


bd_anexo1 = pd.read_excel(os.path.join(r'Z:\PMG 2024-2026\13. Proyecto de Ley\Ley Ppto 2024 - Nº 31953', 'Anexo II - LeyPpto 2024' + ".xlsx"))
bd_anexo5 = pd.read_excel(os.path.join(r'Z:\PMG 2024-2026\13. Proyecto de Ley\Ley Ppto 2024 - Nº 31953', 'Anexo VI - LeyPpto 2024' + ".xlsx"))


# ### <span style="color:blue"> 5. Datos de Base emergencia </span>
# 
# Otras fuentes de información para estructurar la base de datos final

# In[11]:


bd_emerg = pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 3\Bases', 'BASE DE DATOS EMERGENCIAS' + ".xlsx"), dtype={'UBIGEO':str})


# In[12]:


bd_emerg.head()


# ### <span style="color:blue"> 6. Datos de Finalidad Art 64 </span>
# 
# #Otras fuentes de información para estructurar la base de datos final

# In[13]:


bd_fin64 = pd.read_excel(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 3\Bases', 'FINALIDADES ART 64' + ".xlsx"))


# ## <span style="color:green"> SEGUNDA SECCIÓN: Limpieza y llevar la información a la unidad de análisis </span>

# ### 0. Información de la cadena institucional
# 
# Sec ejec: incorporamos algunas ejecutoras que se crearon y creamos variables para identificar el pliego

# In[14]:


sec_ejec = bsec_ejec.copy()
sec_ejec['SEC_EJEC'] = sec_ejec['SEC_EJEC'].astype(int).astype(str)
df = pd.DataFrame([['1. GOBIERNO NACIONAL', '1749', '01. PRESIDENCIA CONSEJO MINISTROS', '001. PRESIDENCIA DEL CONSEJO DE MINISTROS',
                    '021. PROYECTO ESPECIAL LEGADO'], 
                   ['1. GOBIERNO NACIONAL', '1748', '10. EDUCACION', '563. U.N. TECNOLÓGICA DE FRONTERA SAN IGNACIO DE LOYOLA',
                    '001. UNIVERSIDAD NACIONAL TECNOLÓGICA DE FRONTERA SAN IGNACIO DE LOYOLA'],
                   ['1. GOBIERNO NACIONAL', '1745', '22. MINISTERIO PUBLICO', '022. MINISTERIO PUBLICO',
                    '012. AUTORIDAD NACIONAL DE CONTROL DEL MINISTERIO PÚBLICO'],
                   ['2. GOBIERNOS REGIONALES', '1742', '99. GOBIERNOS REGIONALES', '450. GOBIERNO REGIONAL DEL DEPARTAMENTO DE JUNIN',
                    '313. EDUCACIÓN RÍO ENE MANTARO'],
                   ['2. GOBIERNOS REGIONALES', '1743', '99. GOBIERNOS REGIONALES', '445. GOBIERNO REGIONAL DEL DEPARTAMENTO DE CAJAMARCA',
                    '411. HOSPITAL SANTA MARÍA DE CUTERVO'],
                   ['2. GOBIERNOS REGIONALES', '1744', '99. GOBIERNOS REGIONALES', '445. GOBIERNO REGIONAL DEL DEPARTAMENTO DE CAJAMARCA',
                    '412. RED DE SALUD CAJABAMBA'],
                   ['3. GOBIERNOS LOCALES', '350073', '97. MANCOMUNIDADES MUNICIPALES', '073. MANCOMUNIDAD MUNICIPAL PROVINCIAL DEL CENTRO DE AYACUCHO',
                    '073. MANCOMUNIDAD MUNICIPAL PROVINCIAL DEL CENTRO DE AYACUCHO']], columns=['NIVEL_GOB', 'SEC_EJEC', 'SECTOR', 'PLIEGO', 'EJECUTORA'])

sec_ejec = pd.concat([sec_ejec, df], ignore_index=True)
sec_ejec = sec_ejec.drop_duplicates(subset=['SEC_EJEC'])
dupsec = sec_ejec[sec_ejec.duplicated()]
sec_ejec['COD_PLIEGO'] = np.where(sec_ejec['NIVEL_GOB'].str[0]=='3', 
                                  sec_ejec['SECTOR'].str[0:2] + sec_ejec['PLIEGO'].str[0:2] + sec_ejec['EJECUTORA'],
                                  sec_ejec['SECTOR'].str[0:2] + sec_ejec['PLIEGO'])
sec_ejec['ID_PLIEGO'] = sec_ejec['COD_PLIEGO'].str.split('. ',n=1).str[0]
sec_ejec[sec_ejec['NIVEL_GOB'].str[0] == '3'].head()


# In[15]:


prueba_sec = sec_ejec.copy()
prueba_sec = crear_id(prueba_sec, 'NIVEL_GOB', 'SECTOR', 'PLIEGO', 'EJECUTORA')
prueba_sec.head(50)


# ### 1. Información de las notas modificatorias
# 
# Ahora trabajamos la base de datos de notas modificatorias, para incorporar algunas variables de interés y clasificaciones. Para esto sí se requiere conocimiento de qué tipo de modificaciones están vigentes o permitidas por la Ley de PPTO o dispositivos de rango similar

# In[16]:


# Unimos las bases de datos de GN, GR, GL
bd_notas = pd.concat([bd_gn, bd_gr, bd_gl], ignore_index=True)

bd_notas['SEC_EJEC'] = bd_notas['SEC_EJEC'].astype(int).astype(str)
bd_notas['NRO_PLANT'] = bd_notas['NRO_PLANT'].astype(int).astype(str)
bd_notas['MES_EJE'] = bd_notas['MES_EJE'].str[0:2]


# In[17]:


# Check rápido del monto de credito y anulacion
print('CREDITO:', '{:,.0f}'.format(bd_notas.MONTO_CREDITO.sum()))
print('ANULACION:', '{:,.0f}'.format(bd_notas.MONTO_ANULACION.sum()))


# In[18]:


bd_notas.columns


# Nos quedaremos con el código (primer dígito) de los tipos de modificaciones, que pueden ser:

# In[19]:


bd_notas['TIPO_MODIF'].value_counts()


# In[20]:


bd_notas['TIPO_MODIF'] = bd_notas['TIPO_MODIF'].str[0]


# Renombramos algunas variables para que coincidan con la nomenclatura de la base de datos de SIAF

# In[21]:


bd_notas.rename(columns={'PROD_PROY':'PRODUCTO_PROYECTO', 'ACT_OBRA':'ACT_OBRA_ACCINV', 'PROG_PPTO':'PROGRAMA_PPTAL'}, inplace=True)


# Limpiamos variables numéricas, pasando variables string a codificación usando los primeros dígitos

# In[22]:


bd_notas['SEC_EJEC'] = bd_notas['SEC_EJEC'].astype(int).astype(str)
bd_notas['CODIGO_UNICO'] = bd_notas['PRODUCTO_PROYECTO'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_ACTIVIDAD'] = bd_notas['ACT_OBRA_ACCINV'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_FINALIDAD'] = bd_notas['FINALIDAD'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_PROGPPTAL'] = bd_notas['PROGRAMA_PPTAL'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_FUENTE'] = bd_notas['FUENTE'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_RUBRO'] = bd_notas['RUBRO'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_CATEGORIA'] = bd_notas['CATEGORIA'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_GENERICA'] = bd_notas['GENERICA'].str.split('. ',n=1).str[0]
bd_notas['CODIGO_DISPOSLEGAL'] = bd_notas['DISPOSITIVO_LEGAL'].str.split('. ',n=1).str[0].fillna('NA')
bd_notas['TIPO_PROD_PROY'] = np.where(bd_notas['CODIGO_UNICO'].str.startswith('2'),'PROYECTOS','PRODUCTOS')


# Creamos una etiqueta para reconocer si son reducciones o crédito

# In[23]:


bd_notas['ID_CREDITO_ANULA'] = np.where(bd_notas['MONTO_CREDITO'].fillna(0)>0, 'Credito', np.where(bd_notas['MONTO_ANULACION'].fillna(0)>0, 'Anula', 'NA')) 


# Colapsamos la información, completando los missing value primero para evitar problemas al agrupar

# In[24]:


bd_notas['TIPO_MODIF'] = bd_notas['TIPO_MODIF'].fillna('NA')
bd_notas['DISPOSITIVO_LEGAL'] = bd_notas['DISPOSITIVO_LEGAL'].fillna('NA')
bd_notas['TIPO_INGRESO'] = bd_notas['TIPO_INGRESO'].fillna('NA')
bd_notas['NUM_RESOLUC'] = bd_notas['NUM_RESOLUC'].fillna('NA')


# In[25]:


bd_notas = bd_notas.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA', 'CODIGO_DISPOSLEGAL',
                             'MES_EJE', 'NRO_PLANT', 'TIPO_MODIF', 'TIPO_INGRESO', 'ID_CREDITO_ANULA', 'NUM_RESOLUC']).agg({'PRODUCTO_PROYECTO':'first', 'ACT_OBRA_ACCINV':'first', 'FINALIDAD':'first', 'PROGRAMA_PPTAL':'first', 'FUENTE':'first', 'RUBRO':'first',
                                                                                                                            'GENERICA':'first', 'CATEGORIA':'first', 'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum', 'DISPOSITIVO_LEGAL':'first'}).reset_index()

# de paso creamos una variable para diferenciar los PP de los NoPPPROGRAMA_PPTAL
bd_notas['PP'] = np.where(bd_notas['CODIGO_PROGPPTAL'].isin({'9001', '9002'}), '', 'PP')


# In[26]:


bd_notas.head()


# In[27]:


# Check rápido del monto de credito y anulacion
print('CREDITO:', '{:,.0f}'.format(bd_notas.MONTO_CREDITO.sum()))
print('ANULACION:', '{:,.0f}'.format(bd_notas.MONTO_ANULACION.sum()))


# Incorporamos la información de pliego y ejecutora con la base de sec_ejec

# In[28]:


bd_notas = bd_notas.merge(sec_ejec, on=['SEC_EJEC'], how='left', validate='m:1')


# ### 2. Información presupuestaria del SIAF
# 
# Ahora llevamos la información de SIAF al nivel: SEC_EJEC, Producto/Proyecto, Actividad, Fuente, Rubro, Genérica. Las variables incluirán: PIA, PIM, Certificación, Compromiso anual y Devengado.
# 
# Primero vamos a unir los 3 archivos de los niveles de gobierno:

# In[29]:


bd_siaf = pd.concat([bd_gn_seg, bd_gr_seg, bd_gl_seg], ignore_index=True)

print('PIA: ', '{:,.0f}'.format(bd_siaf.PIA.sum()))
print('PIM: ', '{:,.0f}'.format(bd_siaf.PIM.sum()))
print('Dif: ', '{:,.0f}'.format(bd_siaf.PIM.sum() - bd_siaf.PIA.sum()))


# In[30]:


bd_siaf.columns


# Limpiamos variables numéricas, pasando variables string a codificación usando los primeros dígitos

# In[31]:


bd_siaf['SEC_EJEC'] = bd_siaf['SEC_EJEC'].astype(int).astype(str)
bd_siaf['CODIGO_UNICO'] = bd_siaf['PRODUCTO_PROYECTO'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_ACTIVIDAD'] = bd_siaf['ACT_OBRA_ACCINV'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_FINALIDAD'] = bd_siaf['FINALIDAD'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_PROGPPTAL'] = bd_siaf['PROGRAMA_PPTAL'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_FUENTE'] = bd_siaf['FUENTE'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_RUBRO'] = bd_siaf['RUBRO'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_CATEGORIA'] = bd_siaf['CATEGORIA'].str.split('. ',n=1).str[0]
bd_siaf['CODIGO_GENERICA'] = bd_siaf['GENERICA'].str.split('. ',n=1).str[0]
bd_siaf['TIPO_PROD_PROY'] = np.where(bd_siaf['CODIGO_UNICO'].str.startswith('2'),'PROYECTOS','PRODUCTOS')


# In[32]:


bd_siaf.head()


# Colapsamos al nivel que indicamos

# In[33]:


bd_siaf = bd_siaf.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 
                           'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA']).agg({'PIA':'sum', 'PIM':'sum', 'CERTIFICADO':'sum', 'COMPROMISO_ANUAL':'sum', 
                                                                    'TOTAL_DEVENGADO':'sum', 'FINALIDAD':'first', 'PROGRAMA_PPTAL':'first', 'PRODUCTO_PROYECTO':'first', 'ACT_OBRA_ACCINV':'first',
                                                                    'FUENTE':'first', 'RUBRO':'first', 'CATEGORIA':'first', 'GENERICA':'first', 'TIPO_PROD_PROY':'first'}).reset_index()
bd_siaf.head()


# Creamos un ID que está homogenizado

# In[34]:


bd_siaf = bd_siaf.merge(sec_ejec, on=['SEC_EJEC'], how='left', validate='m:1')
bd_siaf = crear_id(bd_siaf, 'NIVEL_GOB', 'SECTOR', 'PLIEGO', 'EJECUTORA')
bd_siaf.columns


# Finalmente, eliminamos las variables innecesarias. Estas ya están incluidas en la base de modificaciones

# In[35]:


bd_siaf.drop(columns=['NIVEL_GOB', 'SECTOR', 'PLIEGO', 'EJECUTORA', 'FINALIDAD', 'PROGRAMA_PPTAL', 'PRODUCTO_PROYECTO', 'ACT_OBRA_ACCINV', 'FUENTE', 'RUBRO', 'CATEGORIA', 'GENERICA'], inplace=True)


# Un identificador para casos que no tienen PIA a nivel de proyecto

# In[36]:


bd_siaf['PIA_CUI'] = bd_siaf.groupby(['COD_ID', 'CODIGO_UNICO'])['PIA'].transform('sum')
bd_siaf['ID_PIA'] = np.where(bd_siaf['PIA_CUI']>0, 1, 0)


# Creamos una variable "llave" para el tablero

# In[37]:


bd_siaf.COD_ID.unique().tolist()


# In[38]:


bd_siaf['key_siaf'] = bd_siaf[['SEC_EJEC',  'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA']].apply(lambda x: '-'.join(x), axis=1)


# In[39]:


bd_siaf.key_siaf.unique()


# ### 3. Información sobre los dispositivos legales que modifican marco presupuestal
# 
# Se necesita colapsar la información al nivel: (Nivel requerido: ID, CUI, ACT, PP, FF, RR, CAT, GEN, DISPO, CREDANULA)

# In[40]:


dispositivos = bd_dispositivos.copy()
#dispositivos.info()


# In[41]:


dispositivos['SEC_EJEC'] = dispositivos['SEC_EJEC'].astype(int).astype(str)
dispositivos['CODIGO_UNICO'] = dispositivos['PRODUCTO'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_ACTIVIDAD'] = dispositivos['ACTIVIDAD'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_PROGPPTAL'] = dispositivos['PROGRAMA_PPTO'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_FUENTE'] = dispositivos['FTE_FINANC'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_RUBRO'] = dispositivos['RUBRO'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_CATEGORIA'] = dispositivos['CAT_ECON'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_GENERICA'] = dispositivos['GENERICA'].str.split('. ',n=1).str[0]
dispositivos['CODIGO_DISPOSLEGAL'] = dispositivos['NRO_DISPOSITIVO'].astype(int).astype(str)
dispositivos['ID_CREDITO_ANULA'] = np.where(dispositivos['MARCO_PPTAL']>0, 'Credito', np.where(dispositivos['MARCO_PPTAL']<0, 'Anula', 'NA'))


# In[42]:


dispositivos.rename(columns={'DISPOSITIVO':'DISPOSITIVO_LEGAL'}, inplace=True)


# In[43]:


dispositivos = dispositivos.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA',
                                    'CODIGO_DISPOSLEGAL', 'ID_CREDITO_ANULA']).agg({'MARCO_PPTAL':'sum', 'DISPOSITIVO_LEGAL':'first'}).reset_index()
dispositivos.head()


# ### 4. Datos de los anexos de la Ley de presupuesto

# #### Datos del Anexo I que implican transferencias del Gobierno Nacional a los gobiernos subnacionales

# In[44]:


anexo1 = bd_anexo1.copy()
anexo1['ID_PLIEGO'] = anexo1['PLIEGO'].str.split('. ',n=1).str[0]
anexo1['CODIGO_UNICO'] = anexo1['PROYECTO'].str.split('. ',n=1).str[0]
anexo1 = anexo1[['ID_PLIEGO', 'CODIGO_UNICO']]
anexo1.head()


# #### Datos del Anexo 5 que se origina del debate con el Congreso

# In[45]:


anexo5 = bd_anexo5.copy()
anexo5['CODIGO_UNICO'] = anexo5['PROYECTO'].str.split('. ',n=1).str[0]
anexo5 = anexo5[['CODIGO_UNICO']].drop_duplicates()
anexo5.head()


# ### 5. Datos del seguimiento de dispositivos de emergencia

# In[46]:


bd_emerreg = bsec_ejec[['NIVEL_GOB', 'SECTOR', 'UBIGEO', 'PLIEGO', 'EJECUTORA', 'SEC_EJEC']].drop_duplicates(subset=['SEC_EJEC'])
bd_emerreg['UBIGEO_DEP'] = np.where(bsec_ejec['NIVEL_GOB'].str[0]=='3',
                             bsec_ejec['SECTOR'].str[0:2] + '0000',
                             "00")
bd_emerreg['GORE'] = np.where(bd_emerreg.NIVEL_GOB.isin({'3. GOBIERNOS LOCALES'}), 'GOBIERNO REGIONAL DE' + bd_emerreg['SECTOR'].str[3:], ' ')

emerg = bd_emerg.copy()
emerg['UBIGEO'] = emerg['UBIGEO'].astype(str)
emerg = emerg.merge(bd_emerreg, on = ['UBIGEO'], how = "left")
emerg['UBIGEO'] = emerg['UBIGEO'].astype(int).astype(str)
emerg['UBIGEO'] = np.where(emerg['UBIGEO'].str.len()!=6, '0' + emerg['UBIGEO'], emerg['UBIGEO'])

emerg = emerg[["UBIGEO","EMERGENCIA_HOY","DIAS","EMERGENCIA_2024"]]

emerg_reg = emerg.copy()
emerg_reg['UBIGEO'] = emerg_reg['UBIGEO'].str[0:2] + '0000'
emerg_reg = emerg_reg[emerg_reg['EMERGENCIA_HOY'] == 'SI']
emerg_reg = emerg_reg.drop_duplicates(subset=['UBIGEO'])
emerg_reg

emerg['ESTADO_EMERGENCIA'] = np.where(emerg['EMERGENCIA_HOY'] == 'SI', 'SI ACTUALMENTE', 
                                      np.where((emerg['EMERGENCIA_HOY'] == 'NO')&(emerg['EMERGENCIA_2024']=='SI DECLARADO 2024'), 'CULMINADO','NO DECLARADO'))

emerg = pd.concat([emerg, emerg_reg], ignore_index=True)

emerg.head()


# ### 6. Datos de las finalidades del Art. 64

# In[47]:


bd_finalidades64 =bd_fin64.copy()
bd_finalidades64['CODIGO_FINALIDAD'] = bd_finalidades64['FINALIDAD_ART_64'].str.split('. ',n=1).str[0]
bd_finalidades64['FINALIDAD64'] = bd_finalidades64['FINALIDAD_ART_64'].str.split('. ',n=1).str[1:]
bd_finalidades64.head()


# ## <span style="color:green"> TERCERA SECCIÓN: UNION </span>

# ### Unión de las bases de datos al nivel de SIAF para comparativa (nos ayudará para una validación posterior)

# In[48]:


# primero llevamos la base de notas a un nivel similar al de la base siaf
bd_notas_siaf = bd_notas.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA']).agg({'PRODUCTO_PROYECTO':'first', 'ACT_OBRA_ACCINV':'first', 'FUENTE':'first', 'RUBRO':'first',
                                                                                                                                                                                         'GENERICA':'first', 'CATEGORIA':'first', 'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum'}).reset_index()

# ahora unimos ambas bases
bd_union = bd_notas_siaf.merge(bd_siaf, on=['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA'], how='outer', validate='1:1')
bd_union = postmerge(bd_union)

bd_union.dtypes


# In[49]:


# Check rápido del monto de credito y anulacion
print('CREDITO:', '{:,.0f}'.format(bd_union.MONTO_CREDITO.sum()))
print('ANULACION:', '{:,.0f}'.format(bd_union.MONTO_ANULACION.sum()))


# ### Otra base de datos para verificación de dispositivos legales

# In[50]:


bd_notas.head()


# In[51]:


# Creamos la base para DLs
bd_notas_dl = bd_notas.copy()

# no consideremos las notas de tipo 3 y 4
bd_notas_dl = bd_notas_dl[bd_notas_dl['TIPO_MODIF'].isin({'1', '2', '7'})] 

# Nos quedamos solo con los que tienen información para dispositivos legales
bd_notas_dl['DISPOSITIVO_LEGAL'] = bd_notas_dl['DISPOSITIVO_LEGAL'].str.split('. ',n=1).str[1].fillna('NAN')
bd_notas_dl = bd_notas_dl[bd_notas_dl['DISPOSITIVO_LEGAL']!='NAN']


# #### Evaluamos las modificaciones de tipo 2 y 7

# In[52]:


bd_notas_dl[bd_notas_dl['TIPO_MODIF']=='2'][['DISPOSITIVO_LEGAL', 'CODIGO_DISPOSLEGAL']].drop_duplicates(subset=['CODIGO_DISPOSLEGAL']).sort_values(by=['CODIGO_DISPOSLEGAL']).head()


# In[53]:


bd_notas_dl[bd_notas_dl['TIPO_MODIF']=='7'][['DISPOSITIVO_LEGAL', 'CODIGO_DISPOSLEGAL']].drop_duplicates(subset=['CODIGO_DISPOSLEGAL']).sort_values(by=['CODIGO_DISPOSLEGAL']).head()


# #### Haremos una primera verificación, al nivel más detallado

# In[55]:


# llevamos la base de notas a un nivel similar al de la base Dispositivos legales (la misma unidad de análisis)
bd_notas_dl = bd_notas_dl.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA', 'CODIGO_DISPOSLEGAL', 'ID_CREDITO_ANULA']).agg({'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum', 
                                                                                                                                                                                                                               'DISPOSITIVO_LEGAL':'first', 'TIPO_MODIF':'first'}).reset_index()

# ahora unimos ambas bases. Ojo, que todos los dispositivos legales deberían contar con una modificación. Es decir, no debería haber "right_only"
bd_union2 = bd_notas_dl.merge(dispositivos, on=['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA', 'CODIGO_DISPOSLEGAL', 'ID_CREDITO_ANULA'], how='outer', indicator=True)
bd_union2 = postmerge(bd_union2)


# In[56]:


bd_notas.head()


# In[57]:


bd_union2._merge.value_counts()


# In[58]:


bd_union2[bd_union2['_merge']!='both'].drop_duplicates(subset=['CODIGO_DISPOSLEGAL']).head()


# In[59]:


# Check rápido del monto de credito y anulacion
print('CREDITO:', '{:,.0f}'.format(bd_union2.MONTO_CREDITO.sum()))
print('ANULACION:', '{:,.0f}'.format(bd_union2.MONTO_ANULACION.sum()))


# #### Haremos una segunda verificación, al nivel de código

# In[60]:


# llevamos la base de notas a un nivel similar al de la base Dispositivos legales (la misma unidad de análisis)
bd_notas_dl = bd_notas_dl.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_FUENTE', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA', 'CODIGO_DISPOSLEGAL', 'ID_CREDITO_ANULA']).agg({'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum', 'DISPOSITIVO_LEGAL':'first', 'TIPO_MODIF':'first'}).reset_index()

# ahora unimos ambas bases. Ojo, que todos los dispositivos legales deberían contar con una modificación. Es decir, no debería haber "right_only"
bd_union3 = bd_notas_dl.merge(dispositivos.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_FUENTE', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA', 'CODIGO_DISPOSLEGAL', 'ID_CREDITO_ANULA']).agg({'DISPOSITIVO_LEGAL':'first', 'MARCO_PPTAL':'sum'}).reset_index(), 
                              on=['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_FUENTE', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA', 'CODIGO_DISPOSLEGAL', 'ID_CREDITO_ANULA'], how='outer', validate='1:1', indicator=True)
bd_union3 = postmerge(bd_union3)


# In[61]:


bd_union3._merge.value_counts()


# In[62]:


bd_union3[bd_union3['_merge']!='both'].drop_duplicates(subset=['CODIGO_DISPOSLEGAL']).head()


# ## <span style="color:green"> CUARTA SECCIÓN: VALIDACIONES </span>
# 
# Realizamos validaciones sobre la información de notas modificatorias

# ### 1. La suma de incorporaciones y reducciones en las notas de tipo 3 debe ser igual a 0:

# In[63]:


# A nivel agregado
print(bd_notas[bd_notas['TIPO_MODIF']=='3']['MONTO_CREDITO'].sum() - bd_notas[bd_notas['TIPO_MODIF']=='3']['MONTO_ANULACION'].sum() == 0, '{:,.0f}'.format(bd_notas[bd_notas['TIPO_MODIF']=='3']['MONTO_CREDITO'].sum()), '{:,.0f}'.format(bd_notas[bd_notas['TIPO_MODIF']=='3']['MONTO_ANULACION'].sum()))

# A nivel de ejecutoras
res_tipo3 = bd_notas[bd_notas['TIPO_MODIF']=='3'].groupby(['SEC_EJEC', 'NRO_PLANT']).agg({'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum'}).reset_index()
res_tipo3 = res_tipo3[((res_tipo3['MONTO_CREDITO'].fillna(0)) - (res_tipo3['MONTO_ANULACION'].fillna(0)))!=0]
print(res_tipo3.shape[0] == 0, res_tipo3.shape[0])


# In[64]:


res_tipo3.head(50)


# ### 2. La suma de incorporaciones y reducciones en las notas de tipo 4 debe ser igual a 0 a nivel de pliego:

# In[65]:


# A nivel agregado
print(bd_notas[bd_notas['TIPO_MODIF']=='4']['MONTO_CREDITO'].sum() - bd_notas[bd_notas['TIPO_MODIF']=='4']['MONTO_ANULACION'].sum() == 0, '{:,.0f}'.format(bd_notas[bd_notas['TIPO_MODIF']=='4']['MONTO_CREDITO'].sum()), '{:,.0f}'.format(bd_notas[bd_notas['TIPO_MODIF']=='4']['MONTO_ANULACION'].sum()))

# A nivel de pliegos
res_tipo4 = bd_notas[bd_notas['TIPO_MODIF']=='4'].groupby(['ID_PLIEGO', 'NUM_RESOLUC']).agg({'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum'}).reset_index()
res_tipo4['Diff'] = res_tipo4['MONTO_CREDITO'].fillna(0) - res_tipo4['MONTO_ANULACION'].fillna(0)
res_tipo4 = res_tipo4[((res_tipo4['MONTO_CREDITO'].fillna(0)) - (res_tipo4['MONTO_ANULACION'].fillna(0)))!=0]
print(res_tipo4.shape[0] == 0, res_tipo4.shape[0])


# In[66]:


res_tipo4[res_tipo4['Diff']!=0].head(50)


# Hacemos algunas correcciones manuales, según el reporte anterior

# In[67]:


bd_notas['NUM_RESOLUC'] = bd_notas['NUM_RESOLUC'].replace({'005-2024':'0005-2024',
                                                           '010-2024/G.R.HVCA/GG':'CAS-2024/G.R.HVCA/GG',
                                                           'RD-022-202-INPE/OPP': 'RD-022-2024-INPE/OPP',
                                                            'RGR 0005-2024-GRA-GR' : 'RGR 0005-2024-GRS-GR',
                                                            'RER 0032-2024-EF':'RER 0032-2024',
                                                            '012-2023-GRSM/GRPYP':'012-2024-GRSM/GRPYP',
                                                            'RGRR 070-2024-GRA/GG':'RGR 0004-2024-GRA-GR' })


# In[68]:


res_tipo4 = bd_notas[bd_notas['TIPO_MODIF']=='4'].groupby(['ID_PLIEGO', 'NUM_RESOLUC']).agg({'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum'}).reset_index()
res_tipo4['Diff'] = res_tipo4['MONTO_CREDITO'].fillna(0) - res_tipo4['MONTO_ANULACION'].fillna(0)
res_tipo4 = res_tipo4[((res_tipo4['MONTO_CREDITO'].fillna(0)) - (res_tipo4['MONTO_ANULACION'].fillna(0)))!=0]
print(res_tipo4.shape[0] == 0, res_tipo4.shape[0])


# In[69]:


res_tipo4[res_tipo4['Diff']!=0].head(50)


# ### 3. El PIM debe ser similar al PIA + Modificaciones:

# In[70]:


bd_union = bd_union.groupby(['SEC_EJEC', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA']).agg({'PIA':'sum', 'MONTO_CREDITO':'sum', 'MONTO_ANULACION':'sum', 'PIM':'sum'}).reset_index()
bd_union['PIA_Modificado'] = bd_union[['PIA', 'MONTO_CREDITO']].sum(axis=1) - bd_union['MONTO_ANULACION'].fillna(0) 
bd_union['Validacion'] = (bd_union['PIA_Modificado'] == bd_union['PIM'])

bd_union['Validacion'].value_counts()


# In[71]:


print(' PIA       ', '{:,.0f}'.format(bd_union.PIA.sum()), '\n Creditos  ', '{:,.0f}'.format(bd_union.MONTO_CREDITO.sum()), '\n Anulacion ', '{:,.0f}'.format(bd_union.MONTO_ANULACION.sum()), '\n PIM       ', '{:,.0f}'.format(bd_union.PIM.sum()), '\n DIF       ', '{:,.0f}'.format(bd_union.PIA.sum() + bd_union.MONTO_CREDITO.sum() - bd_union.MONTO_ANULACION.sum() - bd_union.PIM.sum()))


# ### 4. Verificar que las modificaciones por dispositivos legales sea igual a la modificación de marco de la base de dispositivos

# In[72]:


bd_union2['MontoModif'] = bd_union2['MONTO_CREDITO'].fillna(0) + bd_union2['MONTO_ANULACION'].fillna(0)
bd_union2['MARCO_PPTAL'] = np.where(bd_union2['MARCO_PPTAL']<0, bd_union2['MARCO_PPTAL']*(-1), bd_union2['MARCO_PPTAL'])
bd_union2['Validacion'] = (bd_union2['MARCO_PPTAL'] == bd_union2['MontoModif'])

bd_union2['Validacion'].value_counts()


# In[73]:


bd_union3['MontoModif'] = bd_union3['MONTO_CREDITO'].fillna(0) + bd_union3['MONTO_ANULACION'].fillna(0)
bd_union3['MARCO_PPTAL'] = np.where(bd_union3['MARCO_PPTAL']<0, bd_union3['MARCO_PPTAL']*(-1), bd_union3['MARCO_PPTAL'])
bd_union3['Validacion'] = (bd_union3['MARCO_PPTAL'] == bd_union3['MontoModif'])

bd_union3['Validacion'].value_counts()


# ## <span style="color:green"> QUINTA SECCIÓN: BASE FINAL Y VERIFICACIÓN DE ETIQUETAS </span>

# ### El tablero se nutre principalmente de la base de notas modificatorias. Para ello, vamos a crear indicadores adicionales que permitan la búsqueda de reportes de modificaciones presupuestarias rápida (para los fines del equipo de Inversiones). En ese sentido, necesitamos identificar los tipos de modificaciones (según las autorizaciones vigentes según la Ley de presupuesto o dispositivos como Leyes)
# 
# ### El tablero insume 2 archivos excel: Notas modificatorias & SIAF
# ### La informaciòn de las notas modificatorias ya incluye información del banco de inversiones para proyectos, identificadores de los anexos de la Ley
# 
# #### Para ello contaríamos en el tablero con la llave a nivel de: ID - PROGPPTAL - CODIGO - ACTIVIDAD - FUENTE - RUBRO - CATEGORIA - GENERICA 

# In[74]:


bd_notas_final = bd_notas.copy()
bd_notas_final = crear_id(bd_notas_final, 'NIVEL_GOB', 'SECTOR', 'PLIEGO', 'EJECUTORA')
bd_notas_final['key_siaf'] = bd_notas_final[['SEC_EJEC', 'CODIGO_FINALIDAD', 'CODIGO_PROGPPTAL', 'CODIGO_UNICO', 'CODIGO_ACTIVIDAD', 'CODIGO_FUENTE', 'CODIGO_RUBRO', 'CODIGO_CATEGORIA', 'CODIGO_GENERICA']].apply(lambda x: '-'.join(x), axis=1)
bd_notas_final.head()


# #### Creamos variables para identificar el pliego fácilmente en los 3 niveles

# In[75]:


bd_notas_final['COD_PLIEGO'] = np.where(bd_notas_final['NIVEL_GOB'].str[0]=='3', 
                                        bd_notas_final['SECTOR'].str[0:2] + bd_notas_final['PLIEGO'].str[0:2] + bd_notas_final['EJECUTORA'],
                                        bd_notas_final['SECTOR'].str[0:2] + bd_notas_final['PLIEGO'])
bd_notas_final['ID_PLIEGO'] = bd_notas_final['COD_PLIEGO'].str.split('. ',n=1).str[0]


# #### Incorporamos etiquetas por el anexo I y el anexo 5 (post debate con Congreso)

# In[76]:


bd_notas_final = bd_notas_final.merge(anexo1, on=['ID_PLIEGO', 'CODIGO_UNICO'], how='left', validate='m:1', indicator=True)
bd_notas_final['_merge'] = bd_notas_final['_merge'].replace({'left_only':'No', 'both':'Si', 'right_only':''})
bd_notas_final.rename(columns={'_merge':'AnexoI'}, inplace=True)

bd_notas_final = bd_notas_final.merge(anexo5, on=['CODIGO_UNICO'], how='left', validate='m:1', indicator=True)
bd_notas_final['_merge'] = bd_notas_final['_merge'].replace({'left_only':'No', 'both':'Si', 'right_only':''})
bd_notas_final.rename(columns={'_merge':'Anexo5C'}, inplace=True)


# ### Creamos etiquetas para segmentar las Notas de TIPO 3
# 
# Creamos una variable para identificar qué tipo de modificaciones

# In[77]:


# Se identifican por las variables: SEC_EJEC + TIPO_MODIF + NRO_PLANT 

# ---> Notas de emergencia: "habilita actividades de emergencia"
bd_notas_final['ACT_EMERG'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006144'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_EMERG'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_EMERG'].transform('max')

# Notas de emergencia: "modifica el marco de actividades de emergencia a nivel de Actividad, Rubro y SEC_EJEC"
bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')

bd_notas_final['ACT_EMERG_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006144'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (bd_notas_final['TIPO_MODIF']=='3'), 1, 0)

bd_notas_final['ACT_EMERG_AC'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_EMERG_AC'].transform('max')
bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)

# ---> Notas de emergencia: "habilita actividades de FEN - Atencion"
bd_notas_final['ACT_FEN_AT'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5005827'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_FEN_AT'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_FEN_AT'].transform('max')

# Notas de emergencia: "modifica el marco de actividades de emergencia a nivel de Actividad, Rubro y SEC_EJEC"
bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')

bd_notas_final['ACT_FEN_AT_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5005827'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_FEN_AT_AC'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_FEN_AT_AC'].transform('max')
bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)

# ---> Notas de emergencia: "habilita actividades de FEN - Intervencion"
bd_notas_final['ACT_FEN_IN'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006412'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_FEN_IN'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_FEN_IN'].transform('max')

# Notas de emergencia: "modifica el marco de actividades de emergencia a nivel de Actividad, Rubro y SEC_EJEC"
bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')

bd_notas_final['ACT_FEN_IN_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006412'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_FEN_IN_AC'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_FEN_IN_AC'].transform('max')
bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)



# Notas de prevencion: "habilita actividades de prevención"
bd_notas_final['ACT_PREVE'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5005564', '5005562', '5005571', '5005567', '5005568', '5005580', '5005565', '5005570', '5005585', '5004280'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_PREVE'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_PREVE'].transform('max')

# Notas para mantenimiento: "habilita actividades de mantenimiento"
bd_notas_final['ACT_MANTE'] = np.where((bd_notas_final['CODIGO_UNICO'].str[0].isin({'3'}))&(bd_notas_final['ACT_OBRA_ACCINV'].str.contains('MANTENIMIENTO'))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0) # PARA ACTIVIDADES
bd_notas_final['ACT_MANTE'] = np.where((bd_notas_final['CODIGO_UNICO'].str[0].isin({'3'}))&(bd_notas_final['PRODUCTO_PROYECTO'].str.contains('MANTENIMIENTO'))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, bd_notas_final['ACT_MANTE']) # PARA PRODUCTOS
bd_notas_final['ACT_MANTE'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_MANTE'].transform('max')

# Notas par investigacion de universidades: "habilita actividades de mantenimiento"
bd_notas_final['ACT_INVES'] = np.where((bd_notas_final['PLIEGO'].str[0]=='5')&(bd_notas_final['CODIGO_UNICO'].str[0].isin({'3'}))&(bd_notas_final['ACT_OBRA_ACCINV'].str.contains('INVESTIGA'))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0) # PARA ACTIVIDADES
bd_notas_final['ACT_INVES'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_INVES'].transform('max')

# Notas de servicio de la deuda: "habilita actividades para el pago de deuda"
bd_notas_final['ACT_DEUDA'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5000375', '5000376'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_DEUDA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_DEUDA'].transform('max')

# Notas para habilitar transferencias financieras: "habilita actividades para transferencia financiera"
bd_notas_final['ACT_TRFIN'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5001253'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_TRFIN'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_TRFIN'].transform('max')

# Notas para habilitar preinversion: "habilita pre inversión"
bd_notas_final['ACT_PREIN'] = np.where((bd_notas_final['CODIGO_UNICO'].isin({'2001621'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='3'), 1, 0)
bd_notas_final['ACT_PREIN'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_PREIN'].transform('max')


# In[78]:


bd_notas_final.ACT_PREIN.value_counts()


# ### Creamos etiquetas para segmentar las Notas de TIPO 4
# 
# Creamos una variable para identificar qué tipo de modificaciones

# In[79]:


# Se identifican por la variable: PLIEGO + TIPO_MODIF + NUM_RESOLUC

# Notas de emergencia: "habilita actividades de emergencia"
bd_notas_final['ACT_EMERG'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006144'}))&
                                       (bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&
                                       (bd_notas_final['TIPO_MODIF']=='4'), 1, 
                                       bd_notas_final['ACT_EMERG'])
bd_notas_final['ACT_EMERG'] = np.where(bd_notas_final['TIPO_MODIF']=='4',
                                       bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_EMERG'].transform('max'), 
                                       bd_notas_final['ACT_EMERG'])

# Notas de emergencia: "modifica el marco de actividades de emergencia a nivel de Actividad, Rubro y SEC_EJEC"
bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')

bd_notas_final['ACT_EMERG_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006144'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_EMERG_AC'])
bd_notas_final['ACT_EMERG_AC'] = np.where(bd_notas_final['TIPO_MODIF']=='4', 
                                          bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_EMERG_AC'].transform('max'),
                                          bd_notas_final['ACT_EMERG_AC'])
bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)


# ---> Notas de emergencia: "habilita actividades de FEN - Atencion"
bd_notas_final['ACT_FEN_AT'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5005827'}))&
                                        (bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&
                                        (bd_notas_final['TIPO_MODIF']=='4'), 1, 
                                        bd_notas_final['ACT_FEN_AT'])
bd_notas_final['ACT_FEN_AT'] = np.where(bd_notas_final['TIPO_MODIF']=='4', 
                                        bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_FEN_AT'].transform('max'),
                                        bd_notas_final['ACT_FEN_AT'])

# A NIVEL DE CADENA
bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')

bd_notas_final['ACT_FEN_AT_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5005827'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_FEN_AT_AC'])
bd_notas_final['ACT_FEN_AT_AC'] = np.where(bd_notas_final['TIPO_MODIF']=='4', 
                                          bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_FEN_AT_AC'].transform('max'),
                                          bd_notas_final['ACT_FEN_AT_AC'])
bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)


# ---> Notas de emergencia: "habilita actividades de FEN - Intervencion"
bd_notas_final['ACT_FEN_IN'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006412'}))&
                                        (bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&
                                        (bd_notas_final['TIPO_MODIF']=='4'), 1, 
                                        bd_notas_final['ACT_FEN_IN'])
bd_notas_final['ACT_FEN_IN'] = np.where(bd_notas_final['TIPO_MODIF']=='4', 
                                        bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_FEN_IN'].transform('max'),
                                        bd_notas_final['ACT_FEN_IN'])

# A NIVEL DE CADENA
bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')

bd_notas_final['ACT_FEN_IN_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006412'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_FEN_IN_AC'])
bd_notas_final['ACT_FEN_IN_AC'] = np.where(bd_notas_final['TIPO_MODIF']=='4', 
                                          bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_FEN_IN_AC'].transform('max'),
                                          bd_notas_final['ACT_FEN_IN_AC'])
bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)



# Notas de prevencion: "habilita actividades de prevención"
bd_notas_final['ACT_PREVE'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5005564', '5005562', '5005571', '5005567', '5005568', '5005580', '5005565', '5005570', '5005585', '5004280'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_PREVE'])
bd_notas_final['ACT_PREVE'] = np.where(bd_notas_final['TIPO_MODIF']=='4', bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_PREVE'].transform('max'), bd_notas_final['ACT_PREVE'])

# Notas para mantenimiento: "habilita actividades de mantenimiento"
bd_notas_final['ACT_MANTE'] = np.where((bd_notas_final['CODIGO_UNICO'].str[0].isin({'3'}))&(bd_notas_final['ACT_OBRA_ACCINV'].str.contains('MANTENIMIENTO'))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_MANTE']) # PARA ACTIVIDADES
bd_notas_final['ACT_MANTE'] = np.where((bd_notas_final['CODIGO_UNICO'].str[0].isin({'3'}))&(bd_notas_final['PRODUCTO_PROYECTO'].str.contains('MANTENIMIENTO'))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_MANTE']) # PARA PRODUCTOS
bd_notas_final['ACT_MANTE'] = np.where(bd_notas_final['TIPO_MODIF']=='4', bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_MANTE'].transform('max'), bd_notas_final['ACT_MANTE'])

# Notas par investigacion de universidades: "habilita actividades de mantenimiento"
bd_notas_final['ACT_INVES'] = np.where((bd_notas_final['PLIEGO'].str[0]=='5')&(bd_notas_final['CODIGO_UNICO'].str[0].isin({'3'}))&(bd_notas_final['ACT_OBRA_ACCINV'].str.contains('INVESTIGA'))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_INVES']) # PARA ACTIVIDADES
bd_notas_final['ACT_INVES'] = np.where(bd_notas_final['TIPO_MODIF']=='4', bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_INVES'].transform('max'), bd_notas_final['ACT_INVES'])

# Notas de servicio de la deuda: "habilita actividades para el pago de deuda"
bd_notas_final['ACT_DEUDA'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5000375', '5000376'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_DEUDA'])
bd_notas_final['ACT_DEUDA'] = np.where(bd_notas_final['TIPO_MODIF']=='4', bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_DEUDA'].transform('max'), bd_notas_final['ACT_DEUDA'])

# Notas para habilitar transferencias financieras: "habilita actividades para transferencia financiera"
bd_notas_final['ACT_TRFIN'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5001253'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_TRFIN'])
bd_notas_final['ACT_TRFIN'] = np.where(bd_notas_final['TIPO_MODIF']=='4', bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_TRFIN'].transform('max'), bd_notas_final['ACT_TRFIN'])

# Notas para habilitar preinversion: "habilita pre inversión"
bd_notas_final['ACT_PREIN'] = np.where((bd_notas_final['CODIGO_UNICO'].isin({'2001621'}))&(bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&(bd_notas_final['TIPO_MODIF']=='4'), 1, bd_notas_final['ACT_PREIN'])
bd_notas_final['ACT_PREIN'] = np.where(bd_notas_final['TIPO_MODIF']=='4', bd_notas_final.groupby(['ID_PLIEGO', 'TIPO_MODIF', 'NUM_RESOLUC'])['ACT_PREIN'].transform('max'), bd_notas_final['ACT_PREIN'])


# In[80]:


# etiquetas para segmentar el resto de tipo de notas


# In[81]:


# Notas de emergencia: "habilita actividades de emergencia"
bd_notas_final['ACT_EMERG'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006144'}))&
                                       (bd_notas_final['ID_CREDITO_ANULA'].isin({'Credito', 'Anula'}))&
                                       (~(bd_notas_final['TIPO_MODIF'].isin({'4', '3'}))), 1, 
                                       bd_notas_final['ACT_EMERG'])
bd_notas_final['ACT_EMERG'] = np.where((~(bd_notas_final['TIPO_MODIF'].isin({'4', '3'}))),
                                       bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_EMERG'].transform('max'), 
                                       bd_notas_final['ACT_EMERG'])

bd_notas_final['MONTO_CREDITO_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_CREDITO'].transform('sum')
bd_notas_final['MONTO_ANULACION_CADENA'] = bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT', 'ACT_OBRA_ACCINV'])['MONTO_ANULACION'].transform('sum')
bd_notas_final['ACT_EMERG_AC'] = np.where((bd_notas_final['ACT_OBRA_ACCINV'].str.split(". ",n=1).str[0].isin({'5006144'}))&
                                          (bd_notas_final['MONTO_CREDITO_CADENA'] != bd_notas_final['MONTO_ANULACION_CADENA'])&
                                          (~(bd_notas_final['TIPO_MODIF'].isin({'4', '3'}))), 1, bd_notas_final['ACT_EMERG_AC'])

bd_notas_final['ACT_EMERG_AC'] = np.where((~(bd_notas_final['TIPO_MODIF'].isin({'4', '3'}))), 
                                          bd_notas_final.groupby(['SEC_EJEC', 'TIPO_MODIF', 'NRO_PLANT'])['ACT_EMERG_AC'].transform('max'),
                                          bd_notas_final['ACT_EMERG_AC'])

bd_notas_final.drop(columns=['MONTO_CREDITO_CADENA', 'MONTO_ANULACION_CADENA'], inplace=True)



# In[82]:


bd_notas_final['ACT_EMERG_AC'] = bd_notas_final['ACT_EMERG_AC'].replace({1:'2. REDISTRIBUCIÓN EXTERNA',
                                                                         0:'1. REDISTRIBUCIÓN INTERNA'})


# In[83]:


bd_notas_final[bd_notas_final['ACT_EMERG_AC'] == '2. REDISTRIBUCIÓN EXTERNA'].head()


# #### Creamos identificadores que resumen otras variables o que faciliten el análisis

# In[84]:


bd_notas_final['TIPO_PROD_PROY'] = np.where(bd_notas_final['CODIGO_UNICO'].str[0]=='2', 'Proyectos', 'Actividades')
bd_notas_final['NRO_PLANT_EJEC'] = bd_notas_final['NRO_PLANT'] + '_' + bd_notas_final['COD_ID']


# ### <span style="color:red"> Aligeramos algunas nomenclaturas a través de otras relaciones</span>

# Segundo creamos bases extra para las relaciones por codificación

# In[85]:


# Region (descentralizados)
bd_region = bd_notas_final[['NIVEL_GOB', 'SECTOR', 'PLIEGO', 'SEC_EJEC']].drop_duplicates(subset=['SEC_EJEC'])
bd_region['IDDPTO'] = np.where(bd_region.NIVEL_GOB.isin({'3. GOBIERNOS LOCALES'}), bd_region['SECTOR'].str[0], '')

cambios_region = {'440':'01', # amazonas
                  '441':'02', # ancash
                  '442':'03', # apurimac
                  '443':'04', # arequipa
                  '444':'05', # ayacucho
                  '445':'06', # cajamarca
                  '446':'08', # cusco
                  '447':'09', # huancavelica
                  '448':'10', # huanuco
                  '449':'11', # ica
                  '450':'12', # junin
                  '451':'13', # la libertad
                  '452':'14', # lambayeuqe
                  '453':'16', # loreto
                  '454':'17', # madre de dios
                  '455':'18', # moquegua
                  '456':'19', # pasco
                  '457':'20', # piura
                  '458':'21', # puno
                  '459':'22', # san martin
                  '460':'23', # tacna
                  '461':'24', # tumbes
                  '462':'25', # ucayali 
                  '463':'15', # lima
                  '464':'07', # callao
                  '465':'15'}

cambios_region2 = {'01':'Amazonas', '02':'Ancash', '03':'Apurimac', '04':'Arequipa', '05':'Ayacucho', '06':'Cajamarca', '07':'Callao', '08':'Cusco',
                   '09':'Huancavelica', '10':'Huanuco', '11':'Ica', '12':'Junin', '13':'La Libertad', '14':'Lambayeque', '15':'Lima', '16':'Loreto', 
                   '17':'Madre de Dios', '18':'Moquegua', '19':'Pasco', '20':'Piura', '21':'Puno', '22':'San Martin', '23':'Tacna', '24':'Tumbes', '25':'Ucayali'}

bd_region['IDDPTO'] = np.where(bd_region.NIVEL_GOB.isin({'3. GOBIERNOS LOCALES'}), bd_region['SECTOR'].str[0:2], '')
bd_region['IDDPTO'] = np.where(bd_region.NIVEL_GOB.isin({'2. GOBIERNOS REGIONALES'}), bd_region['PLIEGO'].str.split('.',n=1).str[0].replace(cambios_region), bd_region['IDDPTO'])
bd_region['IDDPTO'] = np.where(bd_region.NIVEL_GOB.isin({'1. GOBIERNO NACIONAL'}), '00', bd_region.IDDPTO)

bd_region['Departamento'] = bd_region['IDDPTO']
bd_region['Departamento'] = bd_region['Departamento'].replace(cambios_region2)

bd_region = bd_region[['SEC_EJEC', 'IDDPTO', 'Departamento']]


# In[86]:


bd_notas_final.head()


# In[87]:


# Distritos (descentralizados)
bd_notas_final['UBIGEO'] = np.where(bd_notas_final['NIVEL_GOB'].str[0]=='3',
                             bd_notas_final['SECTOR'].str[0:2] + bd_notas_final['PLIEGO'].str[0:2] + bd_notas_final['EJECUTORA'].str[0:2],
                                 np.where(bd_notas_final['NIVEL_GOB'].str[0]=='2',
                                 bd_notas_final['DEPARTAMENTO'].str[0:2] + "0000", "00"))
bd_distritos = bd_notas_final[['NIVEL_GOB', 'UBIGEO', 'SECTOR', 'PLIEGO', 'EJECUTORA', 'SEC_EJEC']].drop_duplicates(subset=['SEC_EJEC'])
bd_distritos['Distritos'] = np.where(bd_distritos.NIVEL_GOB.isin({'3. GOBIERNOS LOCALES'}),  bd_distritos['EJECUTORA'].str[31:], '00')

bd_distritos = bd_distritos[['SEC_EJEC', 'UBIGEO', 'Distritos']]
bd_distritos.head()


# In[88]:


# Tipo de notas 
bd_tiponota = bd_notas_final[['TIPO_MODIF']].drop_duplicates(subset=['TIPO_MODIF'])
bd_tiponota['CODIGO_TIPOMODIF'] = bd_tiponota['TIPO_MODIF']
bd_tiponota['TIPO_MODIF'] = bd_tiponota['TIPO_MODIF'].replace({'1':'Tipo 1 Transferencia de partidas',
                                                               '2':'Tipo 2 Créditos suplementarios',
                                                               '3':'Tipo 3 (Dentro de la UE)',
                                                               '4':'Tipo 4 (Entre UE)',
                                                               '7':'Tipo 7 Reducción de marco'})

# Nombre de los proyectos
bd_denominacioncui = bd_notas_final[['CODIGO_UNICO', 'PRODUCTO_PROYECTO']].drop_duplicates(subset=['CODIGO_UNICO'])

# Tipo Producto proyecto
bd_tipoprod = bd_siaf[['CODIGO_UNICO']].copy()
bd_tipoprod['TIPO_PROD_PROY'] = np.where(bd_tipoprod['CODIGO_UNICO'].str[0]=='2', 'Proyectos', 'Actividades')

# Nombre de las actividades
bd_denominacionact = bd_notas_final[['CODIGO_ACTIVIDAD', 'ACT_OBRA_ACCINV']].drop_duplicates(subset=['CODIGO_ACTIVIDAD'])

# Nombre PP
bd_progpptal = bd_notas_final[['CODIGO_PROGPPTAL', 'PROGRAMA_PPTAL']].drop_duplicates(subset=['CODIGO_PROGPPTAL'])

# Finalidad
bd_finalidad = bd_notas_final[['CODIGO_FINALIDAD', 'FINALIDAD']].drop_duplicates(subset=['CODIGO_FINALIDAD'])

# Fuente
bd_fuente = bd_notas_final[['CODIGO_FUENTE', 'FUENTE']].drop_duplicates(subset=['CODIGO_FUENTE'])

# Rubro
bd_rubros = bd_notas_final[['CODIGO_RUBRO', 'RUBRO']].drop_duplicates(subset=['CODIGO_RUBRO'])
bd_rubros['RUBRO'] = bd_rubros['RUBRO'].replace({'18. CANON Y SOBRECANON, REGALIAS, RENTA DE ADUANAS Y PARTICIPACIONES':'18. CANON',
                                                 '13. DONACIONES Y TRANSFERENCIAS':'13. DyT',
                                                 '00. RECURSOS ORDINARIOS':'00. RO',
                                                 '09. RECURSOS DIRECTAMENTE RECAUDADOS':'09. RDR',
                                                 '15. FONDO DE COMPENSACION REGIONAL - FONCOR':'15. FONCOR',
                                                 '19. RECURSOS POR OPERACIONES OFICIALES DE CREDITO':'19. ROFC',
                                                 '08. IMPUESTOS MUNICIPALES':'08. IM',
                                                 '07. FONDO DE COMPENSACION MUNICIPAL':'07. FONCOMUN',
                                                 '04. CONTRIBUCIONES A FONDOS':'04. CONTRIBUCIONES'})

# Categoria
bd_categoria = bd_notas_final[['CODIGO_CATEGORIA', 'CATEGORIA']].drop_duplicates(subset=['CODIGO_CATEGORIA'])

# Generica
bd_generica = bd_notas_final[['CODIGO_GENERICA', 'GENERICA']].drop_duplicates(subset=['CODIGO_GENERICA'])
bd_generica['GENERICA2'] = 'GG' + bd_notas_final['GENERICA'].str[0]
bd_generica['GENERICA'] = bd_generica['GENERICA'].replace({'6. ADQUISICION DE ACTIVOS NO FINANCIEROS':'6. AANF',
                                                           '3. BIENES Y SERVICIOS':'3. BSySS',
                                                           '1. PERSONAL Y OBLIGACIONES SOCIALES':'1. PyOS',
                                                           '5. OTROS GASTOS':'5. OTROS GASTOS',
                                                           '4. DONACIONES Y TRANSFERENCIAS':'4. DyT',
                                                           '2. PENSIONES Y OTRAS PRESTACIONES SOCIALES':'2. PENSIONES',
                                                           '8. SERVICIO DE LA DEUDA PUBLICA':'8. SDP',
                                                           '7. ADQUISICION DE ACTIVOS FINANCIEROS':'7. AAF',
                                                           '0. RESERVA DE CONTINGENCIA':'0. CONTINGENCIA'})

# Dispositivo legal
bd_disposlegal = bd_notas_final[['CODIGO_DISPOSLEGAL', 'DISPOSITIVO_LEGAL']].drop_duplicates(subset=['CODIGO_DISPOSLEGAL'])

# Meses
bd_meses = bd_notas[['MES_EJE']].drop_duplicates()
bd_meses['MES_DEN'] = bd_meses['MES_EJE']
bd_meses['MES_DEN'] = bd_meses['MES_DEN'].replace({'01':'01. Ene',
                                                   '02':'02. Feb',
                                                   '03':'03. Mar',
                                                   '04':'04. Abr',
                                                   '05':'05. May',
                                                   '06':'06. Jun',
                                                   '07':'07. Jul',
                                                   '08':'08. Ago',
                                                   '09':'09. Set',
                                                   '10':'10. Oct',
                                                   '11':'11. Nov',
                                                   '12':'12. Dic'})


# In[89]:


#Añadimos la variable Estado de emergencia a la Base Sec_Ejec
sec_ejec = sec_ejec.merge(emerg, on = ['UBIGEO'], how = "left")
sec_ejec['EST_EMERGENCIA'] = np.where(sec_ejec['EMERGENCIA_HOY'] == 'SI', 'SI ACTUALMENTE', 
                                      np.where((sec_ejec['EMERGENCIA_HOY'] == 'NO')&(sec_ejec['EMERGENCIA_2024']=='SI DECLARADO 2024'), 'CULMINADO',
                                      np.where((sec_ejec['EMERGENCIA_HOY'] == 'NO')&(sec_ejec['EMERGENCIA_2024']=='NO DECLARADO 2024'), 'NO DECLARADO','NO DECLARADO')))
sec_ejec.drop(columns=["EMERGENCIA_HOY","DIAS","EMERGENCIA_2024", 'ESTADO_EMERGENCIA'], inplace=True) 
sec_ejec.head()


# Simplificamos nomenclaturas

# In[90]:


sec_ejec['COD_PLIEGO2'] = sec_ejec['COD_PLIEGO']

sec_ejec['NIVEL_GOB2'] = sec_ejec['NIVEL_GOB'].str[0]
sec_ejec['NIVEL_GOB2'] = sec_ejec['NIVEL_GOB2'].replace({'1':'GN', '2':'GR', '3':'GL'})

sec_ejec.COD_PLIEGO2 = sec_ejec.COD_PLIEGO2.str.replace('GOBIERNO REGIONAL DEL DEPARTAMENTO DE ','GORE ')
sec_ejec.COD_PLIEGO2 = sec_ejec.COD_PLIEGO2.str.replace('GOBIERNO REGIONAL DE LA PROVINCIA CONSTITUCIONAL DEL ','GORE ')
sec_ejec.COD_PLIEGO2 = sec_ejec.COD_PLIEGO2.str.replace('MUNICIPALIDAD METROPOLITANA DE LIMA','MML')
sec_ejec.COD_PLIEGO2 = sec_ejec.COD_PLIEGO2.str.replace('MUNICIPALIDAD PROVINCIAL ','MP ')
sec_ejec.COD_PLIEGO2 = sec_ejec.COD_PLIEGO2.str.replace('MUNICIPALIDAD DISTRITAL ','MD ')

sec_ejec.COD_PLIEGO2 = sec_ejec.COD_PLIEGO2.replace({'01001. PRESIDENCIA DEL CONSEJO DE MINISTROS':'01001. PCM',
                                                   '01002. INSTITUTO NACIONAL DE ESTADISTICA E INFORMATICA':'01002. INEI',
                                                   '26006. INSTITUTO NACIONAL DE DEFENSA CIVIL':'26006. INDECI',
                                                   '35008. COMISION DE PROMOCION DEL PERU PARA LA EXPORTACION Y EL TURISMO - PROMPERU':'35008. PROMPERU',
                                                   '04004. PODER JUDICIAL':'04004. PODER JUDICIAL',
                                                   '04040. ACADEMIA DE LA MAGISTRATURA':'04040. Academia de la Magistratura',
                                                   '06006. M. DE JUSTICIA Y DERECHOS HUMANOS':'06006. MINJUSDH',
                                                   '03060. ARCHIVO GENERAL DE LA NACION':'03060. AGN',
                                                   '06061. INSTITUTO NACIONAL PENITENCIARIO':'06061. INPE',
                                                   '06067. SUPERINTENDENCIA NACIONAL DE LOS REGISTROS PUBLICOS':'06067. SUNARP',
                                                   '07007. M. DEL INTERIOR':'07007. MININTER',
                                                   '08008. M. DE RELACIONES EXTERIORES':'08008. RREE',
                                                   '09009. M. DE ECONOMIA Y FINANZAS':'09009. MEF',
                                                   '05055. INSTITUTO DE INVESTIGACIONES DE LA AMAZONIA PERUANA':'05055. IIAP',
                                                   '09095. OFICINA DE NORMALIZACION PREVISIONAL-ONP':'09095. ONP',
                                                   '10010. M. DE EDUCACION':'10010. MINEDU',
                                                   '05112. INSTITUTO GEOFISICO DEL PERU':'05112. IGP',
                                                   '03113. BIBLIOTECA NACIONAL DEL PERU':'03113. BNP',
                                                   '01114. CONSEJO NACIONAL DE CIENCIA, TECNOLOGIA E INNOVACION TECNOLOGICA':'01114. CONCYTEC',
                                                   '11011. M. DE SALUD':'11011. MINSA',
                                                   '11131. INSTITUTO NACIONAL DE SALUD':'11131. INS',
                                                   '12012. M. DE TRABAJO Y PROMOCION DEL EMPLEO':'12012. MTPE',
                                                   '13013. MINISTERIO DE DESARROLLO AGRARIO Y RIEGO':'13013. MIDAGRI',
                                                   '13160. SERVICIO NACIONAL DE SANIDAD AGRARIA - SENASA':'13160. SENASA',
                                                   '13163. INSTITUTO NACIONAL DE INNOVACION AGRARIA':'13163. INIA',
                                                   '35180. CENTRO DE FORMACION EN TURISMO':'35180. CENFOTUR',
                                                   '01183. INSTITUTO NACIONAL DE DEFENSA DE LA COMPETENCIA Y DE LA PROTECCION DE LA PROPIEDAD INTELECTUAL':'01183. INDECOPI',
                                                   '37205. SERVICIO NACIONAL DE CAPACITACION PARA LA INDUSTRIA DE LA CONSTRUCCION':'37205. SENCICO',
                                                   '16016. M. DE ENERGIA Y MINAS':'16016. MINEM',
                                                   '16220. INSTITUTO PERUANO DE ENERGIA NUCLEAR':'16220. IPEN',
                                                   '16221. INSTITUTO GEOLOGICO MINERO Y METALURGICO':'16221. INGEMMET',
                                                   '38059. FONDO NACIONAL DE DESARROLLO PESQUERO - FONDEPES':'38059. FONDEPES',
                                                   '38240. INSTITUTO DEL MAR DEL PERU - IMARPE':'38240. IMARPE',
                                                   '38241. INSTITUTO TECNOLOGICO DE LA PRODUCCION - ITP':'38241. ITP',
                                                   '19019. CONTRALORIA GENERAL':'19019. Contraloría General',
                                                   '20020. DEFENSORIA DEL PUEBLO':'20020. Defensoría del Pueblo',
                                                   '21021. JUNTA NACIONAL DE JUSTICIA':'21021. JNJ',
                                                   '22022. MINISTERIO PUBLICO':'22022. Ministerio Público',
                                                   '24024. TRIBUNAL CONSTITUCIONAL':'24024. Tribunal Constitucional',
                                                   '37056. SUPERINTENDENCIA NACIONAL DE BIENES ESTATALES':'37056. SBN',
                                                   '26026. M. DE DEFENSA':'26026. MINDEF',
                                                   '05331. SERVICIO NACIONAL DE METEOROLOGIA E HIDROLOGIA':'05331. SENAMHI',
                                                   '26332. INSTITUTO GEOGRAFICO NACIONAL':'26332. IGN',
                                                   '28028. CONGRESO DE LA REPUBLICA':'28028. Congreso de la República',
                                                   '31031. JURADO NACIONAL DE ELECCIONES':'31031. JNE',
                                                   '32032. OFICINA NACIONAL DE PROCESOS ELECTORALES':'32032. ONPE',
                                                   '33033. REGISTRO NACIONAL DE IDENTIFICACION Y ESTADO CIVIL':'33033. RENIEC',
                                                   '03116. INSTITUTO NACIONAL DE RADIO Y TELEVISION DEL PERU - IRTP':'03116. IRTP',
                                                   '11134. SUPERINTENDENCIA NACIONAL DE SALUD':'11134. SUSALUD',
                                                   '10342. INSTITUTO PERUANO DEL DEPORTE':'10342. IPD',
                                                   '07070. INTENDENCIA NACIONAL DE BOMBEROS DEL PERÚ - INBP':'07070. INBP',
                                                   '39345. CONSEJO NACIONAL PARA LA INTEGRACION DE LA PERSONA CON DISCAPACIDAD - CONADIS':'39345. CONADIS',
                                                   '37211. ORGANISMO DE FORMALIZACION DE LA PROPIEDAD INFORMAL':'37211. COFOPRI',
                                                   '01010. DIRECCION NACIONAL DE INTELIGENCIA':'01010. DINI',
                                                   '01011. DESPACHO PRESIDENCIAL':'01011. Despacho Presidencial',
                                                   '01012. COMISION NACIONAL PARA EL DESARROLLO Y VIDA SIN DROGAS - DEVIDA':'01012. DEVIDA',
                                                   '35035. MINISTERIO DE COMERCIO EXTERIOR Y TURISMO':'35035. MINCETUR',
                                                   '36036. MINISTERIO DE TRANSPORTES Y COMUNICACIONES':'36036. MTC',
                                                   '37037. MINISTERIO DE VIVIENDA, CONSTRUCCION Y SANEAMIENTO':'37037. MVCS',
                                                   '38038. MINISTERIO DE LA PRODUCCION':'38038. PRODUCE',
                                                   '39039. MINISTERIO DE LA MUJER Y POBLACIONES VULNERABLES':'39039. MIMP',
                                                   '08080. AGENCIA PERUANA DE COOPERACION INTERNACIONAL - APCI':'08080. APCI',
                                                   '11135. SEGURO INTEGRAL DE SALUD':'11135. SIS',
                                                   '36214. AUTORIDAD PORTUARIA NACIONAL':'36214. APN',
                                                   '09055. AGENCIA DE PROMOCION DE LA INVERSION PRIVADA':'09055. ProInversión',
                                                   '11136. INSTITUTO NACIONAL DE ENFERMEDADES NEOPLASICAS - INEN':'11136. INEN',
                                                   '01016. CENTRO NACIONAL DE PLANEAMIENTO ESTRATEGICO - CEPLAN':'01016. CEPLAN',
                                                   '13018. SIERRA Y SELVA EXPORTADORA':'13018. SSE',
                                                   '01019. ORGANISMO SUPERVISOR DE LA INVERSION PRIVADA EN TELECOMUNICACIONES':'01019. OSIPTEL',
                                                   '01020. ORGANISMO SUPERVISOR DE LA INVERSION EN ENERGIA Y MINERIA':'01020. OSINERGMIN',
                                                   '01021. SUPERINTENDENCIA NACIONAL DE SERVICIOS DE SANEAMIENTO':'01021. SUNASS',
                                                   '01022. ORGANISMO SUPERVISOR DE LA INVERSION EN INFRAESTRUCTURA DE TRANSPORTE DE USO PUBLICO':'01022. OSITRAN',
                                                   '09057. SUPERINTENDENCIA NACIONAL DE ADUANAS Y DE ADMINISTRACION TRIBUTARIA':'09057. SUNAT',
                                                   '09058. SUPERINTENDENCIA DEL MERCADO DE VALORES':'09058. SMV',
                                                   '09059. ORGANISMO SUPERVISOR DE LAS CONTRATACIONES DEL ESTADO':'09059. OSCE',
                                                   '10111. CENTRO VACACIONAL HUAMPANI':'10111. CVH',
                                                   '05005. M. DEL AMBIENTE':'05005. MINAM',
                                                   '27027. FUERO MILITAR POLICIAL':'27027. FMP',
                                                   '13164. AUTORIDAD NACIONAL DEL AGUA - ANA':'13164. ANA',
                                                   '05050. SERVICIO NACIONAL DE AREAS NATURALES PROTEGIDAS POR EL ESTADO - SERNANP':'05050. SERNANP',
                                                   '05051. ORGANISMO DE EVALUACION Y FISCALIZACION AMBIENTAL - OEFA':'05051. OEFA',
                                                   '01023. AUTORIDAD NACIONAL DEL SERVICIO CIVIL':'01023. SERVIR',
                                                   '01024. ORGANISMO DE SUPERVISION DE LOS RECURSOS FORESTALES Y DE FAUNA SILVESTRE':'01024. OSINFOR',
                                                   '36202. SUPERINTENDENCIA DE TRANSPORTE TERRESTRE DE PERSONAS, CARGA Y MERCANCIAS - SUTRAN':'36202. SUTRAN',
                                                   '03003. M. DE CULTURA':'03003. MINCU',
                                                   '26025. CENTRO NACIONAL DE ESTIMACION, PREVENCION Y REDUCCION DEL RIESGO DE DESASTRES - CENEPRED':'26025. CENEPRED',
                                                   '40040. MINISTERIO DE DESARROLLO E INCLUSION SOCIAL':'40040. MIDIS',
                                                   '10117. SISTEMA NACIONAL DE EVALUACION, ACREDITACION Y CERTIFICACION DE LA CALIDAD EDUCATIVA':'10117. SINEACE',
                                                   '13165. SERVICIO NACIONAL FORESTAL Y DE FAUNA SILVESTRE - SERFOR':'13165. SERFOR',
                                                   '12121. SUPERINTENDENCIA NACIONAL DE FISCALIZACION LABORAL':'12121. SUNAFIL',
                                                   '07072. SUPERINTENDENCIA NACIONAL DE CONTROL DE SERVICIOS DE SEGURIDAD, ARMAS, MUNICIONES Y EXPLOSIVOS DE USO CIVIL':'07072. SUCAMEC',
                                                   '07073. SUPERINTENDENCIA NACIONAL DE MIGRACIONES':'07073. MIGRACIONES',
                                                   '05052. SERVICIO NACIONAL DE CERTIFICACION AMBIENTAL PARA LAS INVERSIONES SOSTENIBLES -SENACE':'05052. SENACE',
                                                   '26335. AGENCIA DE COMPRAS DE LAS FUERZAS ARMADAS':'26335. ACFFAA',
                                                   '11137. 0':'11137. 0',
                                                   '38243. ORGANISMO NACIONAL DE SANIDAD PESQUERA - SANIPES':'38243. SANIPES',
                                                   '37207. ORGANISMO TECNICO DE LA ADMINISTRACION DE LOS SERVICIOS DE SANEAMIENTO':'37207. OTASS',
                                                   '10118. SUPERINTENDENCIA NACIONAL DE EDUCACION SUPERIOR UNIVERSITARIA':'10118. SUNEDU',
                                                   '05056. INSTITUTO NACIONAL DE INVESTIGACION EN GLACIARES Y ECOSISTEMAS DE MONTAÑA':'05056. INAIGEM',
                                                   '38244. INSTITUTO NACIONAL DE CALIDAD - INACAL':'38244. INACAL',
                                                   '09096. CENTRAL DE COMPRAS PÚBLICAS - PERÚ COMPRAS':'09096. Perú Compras',
                                                   '36203. AUTORIDAD DE TRANSPORTE URBANO PARA LIMA Y CALLAO - ATU':'36203. ATU',
                                                   '06068. PROCURADURIA GENERAL DEL ESTADO':'06068. Procuraduría'                                                  
                                                  })


# Ajustamos la base de notas final, eliminando algunas variables que ya estarán relacionadas por otro medio

# In[91]:


bd_notas_final.rename(columns={'TIPO_MODIF':'CODIGO_TIPOMODIF'}, inplace=True)

bd_notas_final.drop(columns=['NIVEL_GOB', 'SECTOR', 'PLIEGO', 'EJECUTORA', 'COD_PLIEGO', 'COD_ID', 'ID_PLIEGO', # El match serà por sec ejec
                             'PRODUCTO_PROYECTO', 'ACT_OBRA_ACCINV', 'PROGRAMA_PPTAL', 'FUENTE', 'RUBRO', 'GENERICA', 'CATEGORIA', # el match será por cada una de las codificaciones
                             'DISPOSITIVO_LEGAL',  #match
                             ], inplace=True) # 'TIPO_PROD_PROY',
bd_notas_final.head()


# Eliminamos variables de SIAF que ya están vinculadas

# In[92]:


bd_siaf.drop(columns=['TIPO_PROD_PROY', 'COD_PLIEGO', 'ID_PLIEGO', 'COD_ID', 'PIA_CUI'], inplace=True)


# ## <span style="color:green"> SEXTA SECCIÓN: EXPORTAR </span>

# In[93]:


bd_notas_final.to_csv(os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 3\Outputs3', 'Reporte_Modificaciones_'+fecha_corte+".csv"), index=False)


# In[94]:


outputFile = os.path.join(r'C:\Users\jchamba\Documents\Jannely\Proyecto 3\Outputs3', 'Reporte_SiafPPTO_'+fecha_corte+".xlsx")
with pd.ExcelWriter(outputFile) as ew:
    bd_siaf.to_excel(ew, sheet_name="Data", index = False)
    sec_ejec.to_excel(ew, sheet_name="CadenaInst", index = False)
    bd_tiponota.to_excel(ew, sheet_name="TipoNota", index = False)
    bd_denominacioncui.to_excel(ew, sheet_name="CUI", index = False)
    bd_denominacionact.to_excel(ew, sheet_name="Actividad", index = False)
    bd_progpptal.to_excel(ew, sheet_name="ProgPP", index = False)
    bd_fuente.to_excel(ew, sheet_name="Fuente", index = False)
    bd_rubros.to_excel(ew, sheet_name="Rubro", index = False)
    bd_categoria.to_excel(ew, sheet_name="Categoria", index = False)
    bd_generica.to_excel(ew, sheet_name="Generica", index = False)
    bd_disposlegal.to_excel(ew, sheet_name="DispLegal", index = False)
    bd_meses.to_excel(ew, sheet_name="Meses", index = False)
    bd_region.to_excel(ew, sheet_name='Regiones', index = False)
    bd_distritos.to_excel(ew, sheet_name='Distrito', index = False)
    bd_tipoprod.to_excel(ew, sheet_name='TipoProd', index = False)
    bd_finalidades64.to_excel(ew, sheet_name='Finalidades64', index = False)
    emerg.to_excel(ew, sheet_name='DS_Emerg', index=False)


# In[95]:


bd_notas_final.columns
bd_notas_final.shape


# In[96]:


sec_ejec.columns

