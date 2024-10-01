import pandas as pd
import numpy as np
from datetime import datetime
import os

def crear_df_cliente() -> pd.DataFrame:
    # Creamos el DataFrame de Clientes
    ROOT_PATH = 'C:/Users/ramag/OneDrive/Escritorio/FABRICA/Base de Datos/'
    df = pd.read_csv(ROOT_PATH+'clientes_deta_sql.csv', encoding='ISO-8859-1', header=None)

    # Definimos los datos para crear el DataFrame filtrado
    id_cliente = df[14]
    cliente = df[15]
    telefono = df[18]
    fecha_alta = df[36]
    provincia = df[22]

    # Creamos el DataFrame filtrado
    df = pd.DataFrame({
        'id_Cliente': id_cliente,
        'Cliente': cliente,
        'Fecha_alta': fecha_alta,
        'Provincia': provincia,
        'Telefono': telefono
        })

    #reemplazamos un numero por que tiene comas
    df['Telefono'] = df['Telefono'].replace('(03329) 492670 (lun, martes y mie)', '(03329) 492670')

    # Renombramos los datos de algunas provincias
    def renombrar_provincias(df:pd.DataFrame) -> pd.DataFrame:
        for pos, valor in enumerate(df['Provincia'].values):
            if valor == 'Capital Federal':
                df.loc[pos, 'Provincia'] = 'Buenos Aires'
            elif valor is np.nan or valor == 'Verificar':
                df.loc[pos, 'Provincia'] = 'Desconocido'
            elif valor == 'EXTERIOR':
                df.loc[pos, 'Provincia'] = 'Exterior'
        return df

    df = renombrar_provincias(df)
    
    return df

def cargar_clientes() -> pd.DataFrame:
    '''Funcion para cargar los datos de los clientes preprocesados'''
    df = crear_df_cliente()

    # Creamos los datos que van a ser usados en el Power Bi, los exportamos como CSV
    BDD_PATH = 'C:/Users/ramag/OneDrive/Escritorio/FABRICA/_bdd/'

    if os.path.exists(BDD_PATH + 'Clientes.csv'):
        with open(BDD_PATH + 'Clientes.csv', 'w') as file:
            df.to_csv(BDD_PATH + 'Clientes.csv', index=False)
    else:
        with open(BDD_PATH + 'Clientes.csv', 'w') as file:
            df.to_csv(BDD_PATH + 'Clientes.csv', index=False)
    
    
def exportar_clientes(df:pd.DataFrame, anio:str, mes:str):
    # Obtener la ruta del directorio actual
    ruta_actual = os.path.dirname(os.path.abspath(__file__))

    # Construir la ruta hacia la carpeta que está al mismo nivel que 'App'
    ruta_datos = os.path.join(ruta_actual, os.pardir, 'Historial')

    # Convertir la ruta relativa a absoluta
    HISTORIAL_PATH = os.path.abspath(ruta_datos)
    
    # Construimos las rutas para las carpetas de año y mes
    ruta_anio = os.path.join(HISTORIAL_PATH, anio)
    ruta_mes = os.path.join(ruta_anio, mes)
    
    # Verificamos si existen las carpetas y si no, las creamos
    if not os.path.exists(ruta_anio):
        os.makedirs(ruta_anio)
        
    if not os.path.exists(ruta_mes):
        os.makedirs(ruta_mes)
    
    # Exportamos el DataFrame como CSV dentro de la carpeta correspondiente al mes
    df.to_csv(os.path.join(ruta_mes, 'Clientes.csv'), index=False)



"""-------------------------------------------------------------------"""

def crear_df_movimientos_devoluciones() -> tuple[pd.DataFrame]:
    # Creamos el DataFrame para Movimientos
    ROOT_PATH = 'C:/Users/ramag/OneDrive/Escritorio/FABRICA/Base de Datos/'
    df = pd.read_csv(ROOT_PATH+'movi_stock_depo_sql.csv', encoding='ISO-8859-1', header=None, names=range(48), low_memory=False)

    # Filtramos unas filas que no nos sirven
    df = df[2:]

    #Preproceso las filas que estan en dolares que estan corridas
    hay_dolar = False
    def correr_izquierda(row):
        return pd.Series(list(row[3:]) + [np.nan]*3)

    try:
        fila_deposito = df[df[0] == 'Deposito'].iloc[0]

        fila_modificada = correr_izquierda(fila_deposito)
        fila_modificada = fila_modificada.drop(index=[1,8,10,11,13])
        fila_modificada = fila_modificada[:9]
        hay_dolar = True
    except IndexError:
        pass

    # Filtramos algunas cosas raras
    df = df[df[0] != 'Deposito']
    df = df[df[26] != 'Moneda:']
    df = df[df[32] != 'Citroen Sedan 5P C3 1.4 i']
    df = df[df[32] != '0']
    df = df[df[45] != 'Total en Moneda Base']
    df = df[df[32] != '3040']
    df = df[df[32] != '696']

    # Definimos los datos para crear el DataFrame filtrado
    comprobante = df.loc[:, 26]
    fecha = df.loc[:, 28]
    id_cliente = df.loc[:, 29]
    cliente = df.loc[:,30]
    id_producto = df.loc[:, 31]
    producto = df.loc[:, 32]
    cantidad = df.loc[:, 33]
    precio = df.loc[:, 35]
    cotizacion_dolar = df.loc[:, 38]

    # Creamos el DataFrame filtrado
    df = pd.DataFrame({
        'Comprobante': comprobante,
        'Fecha': fecha,
        'id_Cliente': id_cliente,
        'Cliente': cliente,
        'id_Producto': id_producto,
        'Producto': producto,
        'Cantidad': cantidad,
        'Precio': precio,
        'CotizacionDolar': cotizacion_dolar
    })

    if hay_dolar:
        df_dol = pd.DataFrame([{
            'Comprobante': fila_modificada.values[0],
            'Fecha': fila_modificada.values[1],
            'id_Cliente': fila_modificada.values[2],
            'Cliente': fila_modificada.values[3],
            'id_Producto': fila_modificada.values[4],
            'Producto': fila_modificada.values[5],
            'Cantidad': fila_modificada.values[6],
            'Precio': fila_modificada.values[7],
            'CotizacionDolar': fila_modificada.values[8]
        }])

        df = pd.concat([df, df_dol], ignore_index=True)

    # Nos fijamos cuales precios estan cotizados en dolares
    df['IsPeso'] = np.where(df['CotizacionDolar'] == 1, True, False)

    # Filtramos los ingresos que no nos sirven
    df = df[(df['Comprobante'] != 'Mov. Stock') & (df['Comprobante'] != 'Mov. Stock Int.')]

    # Llenamos algunos nulos y cambiamos de tipo algunos valores
    df['Cliente'] = df['Cliente'].fillna('Desconocido')
    df['Precio'] = df['Precio'].fillna(0)
    df['id_Cliente'] = df['id_Cliente'].fillna(-1)
    df['Cantidad'] = df['Cantidad'].values.astype(float).astype(int)
    df['id_Cliente'] = df['id_Cliente'].values.astype(int)
    df['Precio'] = df['Precio'].values.astype(float).astype(int)
    df['Fecha'] = df['Fecha'].values.astype(datetime)

    # Transformamos todas las cantidades en positivas
    df['Cantidad'] = abs(df['Cantidad'].values)

    # Filtramos cosas que no son ventas
    df = df[df['Producto'] != 'Implantes a $230 Dante Bongiovani']
    df = df[df['Producto'] != 'Implantes  a $210 Dante Bongiovani']
    df = df[df['Producto'] != 'IMNPLANTES SIN CARGO POR FRAC']
    df = df[df['Producto'] != 'SERVICE MICROMOTOR - PIEZA DE MANO - PEDALES']
    df = df[df['Producto'] != 'DIFERENCIA DE IMPLANTE A 230']
    df = df[df['Producto'] != 'PROMOCION DANTE POR CAJITA FELIZ']
    df = df[df['Producto'] != 'GASTOS FINANCIACION TARJETA EN CUOTAS']
    df = df[(df['Cliente'] != 'FERNANDEZ JOSE (ACA NOOOOOOOOOOOOOOO CARGAR) - OBS') & (df['Cliente'] != 'BENCINI ADRIAN CARLOS. (ACA NO)')]

    #filtramos los ajustes a cuentas y logistica y los ponemos en cantidad 1
    def ajuste_logistica(df:pd.DataFrame) -> pd.DataFrame:
        mask = df['id_Producto'].isin(['LOGISTICA','AJUSTE'])
        df.loc[mask, 'Cantidad'] = 1

    ajuste_logistica(df)

    # Creamos la columna IVA correspondiente a cada producto y comprobante
    df['IVA'] = 1.
    def aplicar_iva(df:pd.DataFrame) -> pd.DataFrame:
        mask = df['Comprobante'].isin(['Factura A Manual', 'Factura B Manual',
        'N.C. A Manual', 'N.C. B Manual','N.D. B Manual',
        'Factura B Electronica', 'Factura A Electronica',
        'N.C. B Electronica', 'N.C. A Electronica'])
        df.loc[mask, 'IVA'] = 1.21
        mask = df['id_Producto'].isin(['1AL1', '1AL2','1AC2','1ACG', '1AE1', '7MM3', '7MM6','6PM6','6PM3','6PEDAL2','6PEDAL6'])
        df.loc[mask, 'IVA'] = 1.105
        mask = df['Comprobante'].isin(['N.C. Int.','N.C. Int. 0002','N.D. Int.','Presupuesto','Anticipo Int.'])
        df.loc[mask, 'IVA'] = 1.

    aplicar_iva(df)

    # Creamos la columna Precio Total y determinamos si es positivo o negativo en funcion del tipo de comprobante
    df['Precio_total'] = ((df['Cantidad'] * df['Precio'])*df['IVA']*df['IsPeso']).astype(int)
    df['Precio_total_dols'] = np.where(df['IsPeso'] == False, (df['Cantidad'] * df['Precio'])*df['IVA'], 0).astype(int)
    def devoluciones_negativas(df:pd.DataFrame) -> pd.DataFrame:
        mask = df['Comprobante'].isin(['N.C. Int.', 'N.C. A Manual', 'N.C. B Manual','N.D. B Manual',
        'N.C. Int. 0002', 'N.C. B Electronica', 'N.C. A Electronica', 'N.D. Int.', 'Anticipo Int.'])
        df.loc[mask, 'Precio_total'] = df.loc[mask, 'Precio_total'] * -1
        df.loc[mask, 'Cantidad'] = df.loc[mask, 'Cantidad'] * -1
        
    devoluciones_negativas(df)

    categorias = []
    subcategorias = []

    for id_producto in df['id_Producto']:

    #SUPRA 
        if id_producto in ['20A31', '20A41', '20A51', '20A52','5CC','5P','5P2','AI3','AI4','AI5','AI6','AS3','AS4','AS5','AS6', 'C2', 'C3', 'C32', 'C33', 'C34', 'C4', 'C42', 'C43', 'C44', 'C52', 'C53', 'C54', 'C62', 'C63', 'C64','CALA3','CALA4','CALA5','CAL3','CAL4','CAL5','CDI','CDI3','CDI4','CDI5','CDI6','CDIR','CDIR3','CDIR4','CDIR5','CDIR6','CDIRC','CDIRT3','CDIRT4','CDIRT5','CDIRT6','CDIRTC','CDIRTC3','CDIRTC4','CDIRTC5','CR2','CR3','CR32','CR33','CR34','CR4','CR42','CR43','CR44','CR52','CR53','CR54','MU32','MU42','MU52','O-R2-003','PC3','PC3L','PC3LB','PC4','PC4L','PC4LB','PC5','PC5L','PC5LB','PS32','PS33','PS34','PS42','PS43','PS44','PS52','PS53','PS54','TB1','TB2','TB3','TB30','TB32','TB33','TB34','TB4','TB40','TB42','TB43','TB44','TB50','TB52','TB53','TB54','TMU3','TMU45', 'TOR', 'TOR2', 'TOR2.3', 'TOR3', 'TORCA3', 'TORCA45']:
            categorias.append('SUPRAESTRUCTURA')

            if id_producto in ['20A31', '20A41', '20A51', '20A52']:
                subcategorias.append('ANGULADA')

            elif id_producto in ['5CC']:
                subcategorias.append('CAZOLETA')

            elif id_producto in ['5P','5P2']:
                subcategorias.append('PIN')

            elif id_producto in ['AI3','AI4','AI5','AI6','AS3','AS4','AS5','AS6']:
                subcategorias.append('ANALOGO')

            elif id_producto in ['C2', 'C3', 'C32', 'C33', 'C34', 'C4', 'C42', 'C43', 'C44', 'C52', 'C53', 'C54', 'C62', 'C63', 'C64']:
                subcategorias.append('CEMENTABLE')

            elif id_producto in ['CALA3','CALA4','CALA5','CAL3','CAL4','CAL5']:
                subcategorias.append('CALCINABLE')

            elif id_producto in ['CDI','CDI3','CDI4','CDI5','CDI6', 'CDIRC']:
                subcategorias.append('UCLA CALCINABLE')

            elif id_producto in ['CDIR','CDIR3','CDIR4','CDIR5','CDIR6']:
                subcategorias.append('UCLA CALCINABLE ANTIRROTACIONAL')

            elif id_producto in ['CDIRT3','CDIRT4','CDIRT5','CDIRT6']:
                subcategorias.append('UCLA TITANIO ANTIRROTACIONAL')

            elif id_producto in ['CDIRTC','CDIRTC3','CDIRTC4','CDIRTC5']:
                subcategorias.append('UCLA CALCINABLE CON BASE METALICA')

            elif id_producto in ['CR2','CR3','CR32','CR33','CR34','CR4','CR42','CR43','CR44','CR52','CR53','CR54']:
                subcategorias.append('ANTIRROTACIONAL')

            elif id_producto in ['MU32','MU42','MU52','TMU3','TMU45']:
                subcategorias.append('MULTI UNIT')

            elif id_producto in ['O-R2-003']:
                subcategorias.append('O RING')

            elif id_producto in ['PC3','PC3L','PC3LB','PC4','PC4L','PC4LB','PC5','PC5L','PC5LB']:
                subcategorias.append('PILAR TI BASE')

            elif id_producto in ['PS32','PS33','PS34','PS42','PS43','PS44','PS52','PS53','PS54']:
                subcategorias.append('PLATAFORMA SWITCHING')

            elif id_producto in ['TB1','TB2','TB3','TB30','TB32','TB33','TB34','TB4','TB40','TB42','TB43','TB44','TB50','TB52','TB53','TB54']:
                subcategorias.append('BALL ATTACH')

            elif id_producto in ['TOR', 'TOR2', 'TOR2.3', 'TOR3', 'TORCA3', 'TORCA45']:
                subcategorias.append('TORNILLO')
                
            else:
                subcategorias.append('OTRO')
    #IMPLANTE        
        elif id_producto in ['HC310','HC311.5','HC313','HC316','HC408','HC410','HC411.5','HC413','HC416','HC508','HC510','HC511.5','HC513','HC516','IA310','IA311.5','IA313','IA316','IA407','IA408','IA410','IA410MM','IA411.5','IA413','IA416','IA507','IA508','IA510','IA511.5','IA513','IA516','IA607','IA610','IA611.5','IA613','IC310','IC311.5','IC313','IC316','IC408','IC410','IC411.5','IC413','IC416','IC508','IC510','IC511.5','IC513','IC516','IPC10','IPC11.5','IPC13','IPC16','IPTB10','IPTB11.5','IPTB13','IPTB16']:
            categorias.append('IMPLANTE')
            
            if id_producto in ['HC310','HC311.5','HC313','HC316','HC408','HC410','HC411.5','HC413','HC416','HC508','HC510','HC511.5','HC513','HC516']:
                subcategorias.append('HC')
                
            elif id_producto in ['IA310','IA311.5','IA313','IA316','IA407','IA408','IA410','IA410MM','IA411.5','IA413','IA416','IA507','IA508','IA510','IA511.5','IA513','IA516','IA607','IA610','IA611.5','IA613']:
                subcategorias.append('IA')

            elif id_producto in ['IC310','IC311.5','IC313','IC316','IC408','IC410','IC411.5','IC413','IC416','IC508','IC510','IC511.5','IC513','IC516']:
                subcategorias.append('COMPATIBLE')

            elif id_producto in ['IPC10','IPC11.5','IPC13','IPC16']:
                subcategorias.append('IPC')

            elif id_producto in ['IPTB10','IPTB11.5','IPTB13','IPTB16']:
                subcategorias.append('IPTB')
                
            else:
                subcategorias.append('OTRO')

    #DESTAPE     
        elif id_producto in ['TC2','TC3','TC32','TC33','TC34','TC4','TC42','TC43','TC44','TC52','TC53','TC54','TC62','TC63','TC64']:
            categorias.append('DESTAPE')
            
            if id_producto in ['TC2','TC3','TC32','TC33','TC34','TC4','TC42','TC43','TC44','TC52','TC53','TC54','TC62','TC63','TC64']:
                subcategorias.append('TAPON DE CICATRIZACION')
                
            else:
                subcategorias.append('OTRO')
    #TRANSFER
        elif id_producto in ['AI3CC','AI4CC','AI5CC','TCA','TCC','TCC3','TCC4','TCC5','TCDIR3','TCDIR4','TCDIR5','TCDIR6']:
            categorias.append('TRANSFER')
            
            if id_producto in ['AI3CC','AI4CC','AI5CC']:
                subcategorias.append('CAD CAM')
                
            elif id_producto in ['TCA','TCDIR3','TCDIR4','TCDIR5','TCDIR6']:
                subcategorias.append('CUBETA ABIERTA')
                
            elif id_producto in ['TCC','TCC3','TCC4','TCC5']:
                subcategorias.append('SCAN BODY')
                
            else:
                subcategorias.append('OTRO')
    #AVIO
        elif id_producto in ['1ACG', '1AC2','1AC1','1AE1','1AL1','1AL2']:
            categorias.append('AVIO')
            
            if id_producto in ['1ACG']:
                subcategorias.append('CIRUGIA GUIADA')
                
            elif id_producto in ['1AC2']:
                subcategorias.append('SEGUNDA CIRUGIA')
                
            elif id_producto in ['1AC1']:
                subcategorias.append('PRIMERA CIRUGIA')
            
            elif id_producto in ['1AL1']:
                subcategorias.append('PRIMERA CIRUGIA C/IRRIGACION')
                
            elif id_producto in ['1AL2']:
                subcategorias.append('PRIMERA CIRUGIA S/IRRIGACION')
                
            elif id_producto in ['1AE1']:
                subcategorias.append('EXPANSION OSEA')
                
            else:
                subcategorias.append('OTRO')
    #INSTRUMENTAL
        elif id_producto in ['2A1', '1AL1','1AL2','2A1', '2A1CG','2A3', '2A4', '2A5', '2A6','2AC', '2C3', '2C4', '2C5', '2C6','2CT1','2CV1','2CV3','2CV2','2D1','2D3','2HC3','2HC4','2HC5','2K0','2K4','2K5','2K6','2K7','2K8','2K9','2KE','2LF1','2MM1','2MM3','2MM45','2P1','2P2','2P23','2P3','2PIN3','2PIN4','2PIN5','2PIN6','2S3','2S4', '2S5','2SHC3','2SHC4','2SHC5','2TT23C','2TT23L','2TT23M','2TT3C','2TT3L','2TT3M','2TT4C','2TT4L','2TT4M','2TT5C','2TT5L','2TT5M','4D3','4L1','4L2','4LH2','4LHC','4LHCC','4LHCL','4LHEL','4LHU','4S1','4S2','4S3','5E1','5E2','5E3','5E4','5E5']:
            categorias.append('INSTRUMENTAL')
            
            if id_producto in ['2A1', '1AL1','1AL2','2A1','2A3', '2A4', '2A5', '2A6','2AC','2CT1','2CV1','2CV3','2CV2','2D1','2D3','2K0','2K4','2K5','2K6','2K7','2K8','2K9','2LF1','2P1','2P2','2P23','2P3','2PIN3','2PIN4','2PIN5','2PIN6','2S3','2S4', '2S5','2SHC3','2SHC4','2SHC5','2TT23C','2TT23L','2TT23M','2TT3C','2TT3L','2TT3M','2TT4C','2TT4L','2TT4M','2TT5C','2TT5L','2TT5M','4A1','4A2']:
                subcategorias.append('INSTRUMENTAL PRIMER CIRUGIA')
                
            elif id_producto in ['2A1CG', '2C3', '2C4', '2C5', '2C6','2HC3','2HC4','2HC5']:
                subcategorias.append('INSTRUMENTAL CIRUGIA GUIADA')
                
            elif id_producto in ['2KE','5E1','5E2','5E3','5E4','5E5']:
                subcategorias.append('INSTRUMENTAL EXPANSION OSEA')
                
            elif id_producto in ['2MM1','2MM3','2MM45','4L1','4L2']:
                subcategorias.append('ACCESORIO')
                
            elif id_producto in ['4A1','4A2','4LH2','4LHC','4LHCC','4LHCL','4LHEL','4LHU','4S1','4S2','4S3']:
                subcategorias.append('INSTRUMENTAL SEGUNDA CIRUGIA')
                
            else:
                subcategorias.append('OTRO')
                
    #MICROMOTOR
        elif id_producto in ['7MM3','7MM6']:
            categorias.append('MICROMOTOR')
            
            if id_producto in ['7MM3']:
                subcategorias.append('3000 F')
                
            elif id_producto in ['7MM6']:
                subcategorias.append('6000 F')
            
    #ACCESORIOS MICROMOTOR
        elif id_producto in ['6PM3','6PM6','6PEDAL2','6PEDAL6']:
            categorias.append('ACCESORIO MICROMOTOR')
            subcategorias.append('ACCESORIO')
        
        else:
            categorias.append('OTRO')
            subcategorias.append('OTRO')

    df['Categoria'] = categorias
    df['Categoria'] = df['Categoria'].fillna('OTRO')
    df['SubCategoria'] = subcategorias
    df['SubCategoria'] = df['SubCategoria'].fillna('OTRO')

    # Creamos un DataFrame de Devoluciones y nos quedamos solo con las devoluciones de menos de 50 productos
    df_devoluciones = df[df['Comprobante'].isin(['N.C. Int.', 'N.C. A Manual', 'N.C. B Manual','N.D. B Manual','N.C. Int. 0002', 'N.C. B Electronica', 'N.C. A Electronica', 'N.D. Int.'])]
    df_devoluciones = df_devoluciones[df_devoluciones['Cantidad'] < 50]

    return (df, df_devoluciones)


def cargar_movimientos_devoluciones(df, df_devoluciones) -> tuple[pd.DataFrame]:
    # Creamos los datos que van a ser usados en el Power Bi, los exportamos como CSV
    BDD_PATH = 'C:/Users/ramag/OneDrive/Escritorio/FABRICA/_bdd/'


    if os.path.exists(BDD_PATH + 'Ventas.csv'):
        with open(BDD_PATH + 'Ventas.csv', 'w') as file:
            df.to_csv(BDD_PATH + 'Ventas.csv', index=False)
    else:
        with open(BDD_PATH + 'Ventas.csv', 'w') as file:
            df.to_csv(BDD_PATH + 'Ventas.csv', index=False)
            
    if os.path.exists(BDD_PATH + 'Devoluciones.csv'):
        with open(BDD_PATH + 'Devoluciones.csv', 'w') as file:
            df_devoluciones.to_csv(BDD_PATH + 'Devoluciones.csv', index=False)
    else:
        with open(BDD_PATH + 'Devoluciones.csv', 'w') as file:
            df_devoluciones.to_csv(BDD_PATH + 'Devoluciones.csv', index=False)


def exportar_movimientos_devoluciones(df:pd.DataFrame, df_devoluciones:pd.DataFrame, anio:str, mes:str):
    # Obtener la ruta del directorio actual
    ruta_actual = os.path.dirname(os.path.abspath(__file__))

    # Construir la ruta hacia la carpeta que está al mismo nivel que 'App'
    ruta_datos = os.path.join(ruta_actual, os.pardir, 'Historial')

    # Convertir la ruta relativa a absoluta
    HISTORIAL_PATH = os.path.abspath(ruta_datos)
    
    # Construimos las rutas para las carpetas de año y mes
    ruta_anio = os.path.join(HISTORIAL_PATH, anio)
    ruta_mes = os.path.join(ruta_anio, mes)
    
    # Verificamos si existen las carpetas y si no, las creamos
    if not os.path.exists(ruta_anio):
        os.makedirs(ruta_anio)
        
    if not os.path.exists(ruta_mes):
        os.makedirs(ruta_mes)
    
    # Exportamos el DataFrame como CSV dentro de la carpeta correspondiente al mes
    df.to_csv(os.path.join(ruta_mes, 'Ventas.csv'), index=False)
    df_devoluciones.to_csv(os.path.join(ruta_mes, 'Devoluciones.csv'), index=False)


#####################################
def crear_ventas_totales_historial():
    def sumar_historiales(ruta_historiales, archivo_total):
        ventas_tot = pd.DataFrame()
        primera_iteracion = True

        for carpeta_raiz, subcarpetas, archivos in os.walk(ruta_historiales):
            for archivo in archivos:
                if archivo == "Ventas.csv":
                    ruta_csv = os.path.join(carpeta_raiz, archivo)
                    
                    if primera_iteracion:
                        df = pd.read_csv(ruta_csv, header=0)
                        columnas = df.columns
                        primera_iteracion = False
                    else:
                        df = pd.read_csv(ruta_csv, header=0)
                        df.columns = columnas 

                    ventas_tot = pd.concat([ventas_tot, df], ignore_index=True)

        # Guardar el DataFrame acumulado en un nuevo archivo CSV
        ventas_tot.to_csv(archivo_total, index=False)
        
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_datos = os.path.join(ruta_actual, os.pardir, 'Historial')
    HISTORIAL_PATH = os.path.abspath(ruta_datos)

    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_datos = os.path.join(ruta_actual, os.pardir, 'HistoricoImplantVel')
    HISTORICO_PATH = os.path.abspath(ruta_datos)

    # Definir el archivo de historial total dentro de la nueva carpeta
    ventas = os.path.join(HISTORICO_PATH, 'Ventas.csv')

    # Ejecutar la función para sumar los historiales
    sumar_historiales(HISTORIAL_PATH, ventas)


'''-------------------------------------------------------------------------------'''

def actualizar_historico():
    # Creamos un Historico donde se van a ir actualizando los datos
    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_datos = os.path.join(ruta_actual, os.pardir, 'Historial')
    HISTORIAL_PATH = os.path.abspath(ruta_datos)

    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_datos = os.path.join(ruta_actual, os.pardir, 'HistoricoImplantVel')
    HISTORICO_PATH = os.path.abspath(ruta_datos)

    ruta_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_datos = os.path.join(ruta_actual, os.pardir, '_bdd')
    BDD_PATH = os.path.abspath(ruta_datos)

    def sumar_historiales(ruta_historiales, ruta_historico, ruta_bdd):
        ventas_tot = pd.DataFrame()
        devoluciones_tot = pd.DataFrame()
        clientes_tot = pd.DataFrame()
        columnas = None
        primera_iteracion = True

        for carpeta_raiz, subcarpetas, archivos in os.walk(ruta_historiales):
            for archivo in archivos:
                if archivo == "Ventas.csv":
                    ruta_csv = os.path.join(carpeta_raiz, archivo)
                    
                    if primera_iteracion:
                        df = pd.read_csv(ruta_csv, header=0)
                        columnas = df.columns 
                        primera_iteracion = False
                    else:
                        df = pd.read_csv(ruta_csv, header=0)
                        
                        if df.shape[1] == len(columnas):
                            df.columns = columnas 

                    ventas_tot = pd.concat([ventas_tot, df], ignore_index=True).drop_duplicates()
                    
                elif archivo == "Devoluciones.csv":
                    ruta_csv = os.path.join(carpeta_raiz, archivo)
                    
                    if primera_iteracion:
                        df = pd.read_csv(ruta_csv, header=0)
                        columnas = df.columns 
                        primera_iteracion = False
                    else:
                        df = pd.read_csv(ruta_csv, header=0)
                        
                        if df.shape[1] == len(columnas):
                            df.columns = columnas 

                    devoluciones_tot = pd.concat([devoluciones_tot, df], ignore_index=True)
                    
                elif archivo == "Clientes.csv":
                    ruta_csv = os.path.join(carpeta_raiz, archivo)
                    
                    if primera_iteracion:
                        df = pd.read_csv(ruta_csv, header=0)
                        columnas = df.columns 
                        primera_iteracion = False
                    else:
                        df = pd.read_csv(ruta_csv, header=0)
                        
                        if df.shape[1] == len(columnas):
                            df.columns = columnas

                    clientes_tot = pd.concat([clientes_tot, df], ignore_index=True)
        
        ventas_tot.to_csv(ruta_historico+"/Ventas.csv", index=False)
        devoluciones_tot.to_csv(ruta_historico+"/Devoluciones.csv", index=False)
        clientes_tot = clientes_tot.drop_duplicates('id_Cliente')
        clientes_tot.to_csv(ruta_historico+"/Clientes.csv", index=False)
        
        if os.path.exists(ruta_bdd + '/Ventas_totales.csv'):
            with open(ruta_bdd + '/Ventas_totales.csv', 'w') as file:
                ventas_tot.to_csv(ruta_bdd + '/Ventas_totales.csv', index=False)
        else:
            with open(ruta_bdd + '/Ventas_totales.csv', 'w') as file:
                ventas_tot.to_csv(ruta_bdd + '/Ventas_totales.csv', index=False)
                
        if os.path.exists(ruta_bdd + '/Devoluciones_totales.csv'):
            with open(ruta_bdd + '/Devoluciones_totales.csv', 'w') as file:
                devoluciones_tot.to_csv(ruta_bdd + '/Devoluciones_totales.csv', index=False)
        else:
            with open(ruta_bdd + '/Devoluciones_totales.csv', 'w') as file:
                devoluciones_tot.to_csv(ruta_bdd + '/Devoluciones_totales.csv', index=False)
                
        if os.path.exists(ruta_bdd + '/Clientes_totales.csv'):
            with open(ruta_bdd + '/Clientes_totales.csv', 'w') as file:
                clientes_tot.to_csv(ruta_bdd + '/Clientes_totales.csv', index=False)
        else:
            with open(ruta_bdd + '/Clientes_totales.csv', 'w') as file:
                clientes_tot.to_csv(ruta_bdd + '/Clientes_totales.csv', index=False)

    sumar_historiales(HISTORIAL_PATH, HISTORICO_PATH, BDD_PATH)

