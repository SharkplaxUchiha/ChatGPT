import cx_Oracle
import config
import sys
import sqlite3
from datetime import datetime, time, timedelta
import pandas as pd
import time

#python getReportCognos.v1.py 23032023 23032023 Nombre_Reporte

#Si no se reciben argumentos, entonces se tomara la fecha del día
if(len(sys.argv) == 4):
    #PQ2 es la fecha de inicio y debe ir con formato: dd-MM-yy
    # Define the date variables
    strFechaInicial = sys.argv[1]
    strFechaFinal = sys.argv[2]  
    # Convertir las fechas ingresadas en objetos de fecha de Python y formatearlas como YYYY-MM-DD
    Fecha_Inicio = datetime.strptime(strFechaInicial, "%d%m%Y").strftime("%d-%b-%y")
    Fecha_Fin = datetime.strptime(strFechaFinal, "%d%m%Y").strftime("%d-%b-%y")  
    strNombreReporte = sys.argv[3]  
else:
    strFechaInicial = datetime.now().date()
    strFechaFinal = datetime.now().date()
    Fecha_Inicio = strFechaInicial.strftime("%d-%b-%y")
    Fecha_Fin = strFechaFinal.strftime("%d-%b-%y")
    strNombreReporte = 'Nombre_Reporte'
    
#Nombre del query
strNombreQuery = strNombreReporte + '.sql'

def validar_fechas(fecha_inicio, fecha_fin):
    
    # Validar que las fechas sean reales y no pasen de la fecha actual
    hoy = datetime.now().date()

    try:
        fecha_inicio = datetime.strptime(fecha_inicio, "%d%m%Y").date()
        fecha_fin = datetime.strptime(fecha_fin, "%d%m%Y").date()
        assert fecha_inicio < hoy, "La fecha de inicio es mayor que la fecha actual"
        assert fecha_fin < hoy, "La fecha final es mayor que la fecha actual"
        assert fecha_inicio < fecha_fin, "La fecha de inicio es mayor que la fecha final"
        assert (fecha_fin - fecha_inicio).days <= 4, "La diferencia entre las fechas es mayor a 4 días"
    except ValueError:
        raise ValueError("Las fechas ingresadas no son válidas")
    except AssertionError as e:
        raise ValueError(str(e))
    else:
        # Las fechas son válidas
        return True

# Generar fecha actual
strFechaActual = datetime.now().strftime('%Y%m%d')

strPathReporte = './'

#Define el nombre del archivo de salida del reporte generado en CSV
strNombreReporte = strNombreReporte + '_' +strFechaActual

print("Fecha Inicio:" + Fecha_Inicio)
print("Fecha Fin:" + Fecha_Fin)
print("Nombre Reporte:" + strNombreReporte)
print("Ruta del Reporte:" + strPathReporte)

connection = None
try:
    connection = cx_Oracle.connect(
        config.username,
        config.password,
        config.db_alias)

    # show the version of the Oracle Database
    print(connection.version)
    
    # Medir el tiempo de ejecución
    inicio_tiempo = time.time()

    c = connection.cursor()
    #c.execute(sqlTesting)

    # Read the query from the .sql file
    with open(strPathReporte + strNombreQuery, 'r', encoding='UTF-8') as file:
        query = file.read()

    # Execute the query with the date variables
    c.execute(query, {'Fecha_Inicio': Fecha_Inicio, 'Fecha_Fin': Fecha_Fin})

    # Obtener los encabezados del resultado
    encabezados = [columna[0] for columna in c.description]

    # Fetch all results
    results = c.fetchall()

    #generamos el frame con los resultados y encabezados
    df = pd.DataFrame(results,columns=encabezados)

    #generación del archivo de salida
    df.to_csv(strPathReporte+strNombreReporte+'.csv', index = False, encoding="ANSI") # place 'r' before the path name

    # Calcular el tiempo de ejecución
    fin_tiempo = time.time()
    tiempo_total = fin_tiempo - inicio_tiempo

    # Imprimir el tiempo de ejecución
    print(f"El proceso tardó {tiempo_total} segundos en ejecutarse")

except cx_Oracle.Error as error:
    print(error)
finally:
    # release the connection
    if connection:
        connection.close()