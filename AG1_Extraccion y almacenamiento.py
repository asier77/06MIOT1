# Este código permite volcar a un fichero local JSON el contenido de un conjunto de datos abiertos alojado en internet.
# El contenido del fichero se carga en MongoDB Atlas para realizar algunas consultas.
# Fichero JSON: M4 Estaciones Metro de Madrid (289 registros)
# Obtenido del portal de Datos Abiertos del Consorcio Regional de Transportes de Madrid : https://data-crtm.opendata.arcgis.com/

# Importamos las librerías necesarias
import urllib.request, json, pymongo, pprint


# Función que recibe por parámetro la url que contiene el dataset
# y el nombre del fichero JSON donde vamos a volcar el contenido.
def urlToJsonFile(urlText,filename):
    # Se accede a la url recibida y se descarga el objeto json
    url = urllib.request.urlopen(urlText)
    data = json.loads(url.read().decode())
    # Abrimos el fichero recibido en modo escritura
    outfile = open(filename,'w')
    # Volcamos los datos leidos al fichero local
    json.dump(data, outfile)
# fin-funcion

# Función que recibe por parámetro el nombre del fichero JSON
# volcado a disco.
def insertJsonDataInDB(filename):
    # Abrimos el fichero en modo lectura
    f = open(filename,'r')
    # Leemos los datos JSON del fichero y los metemos en la variable data_json
    data_json = json.load(f)
    # Seleccionamos del objeto JSON unicamente el array de objetos con los datos
    data = data_json['features']
    # Insertamos cada documento del array en la colección
    col.insert_many(data)
# fin-funcion

# Función que recibe por parámetro el código de la estación y
# devuelve varios campos
def searchInfoByCodigoEstacion(code):
     print("\n\n*** Buqueda de info. por código de estación ***")

     cursor = col.find({"attributes.CODIGOESTACION" : code},
                       { "attributes.DENOMINACION": 1,
                         "attributes.NOMBREVIA": 1,
                         "attributes.NUMEROPORTAL": 1,
                         "attributes.DISTRITO": 1,
                         "attributes.CORONATARIFARIA": 1 })

     for c in cursor:
         pprint.pprint(c)
# fin-funcion

# Función que recibe por parámetro el numero de distrito y devuelve el nombre,
# el distrito y la fecha de alta de las 3 primeras estaciones del distrito
# ordenadas por nombre ascendente y fecha de alta descendente
def searchDistritoTop3(dis):
    print("\n\n*** Buqueda de las 3 primeras estaciones del distrito " + dis + " ***")

    cursor = col.find({"attributes.DISTRITO": dis},
                      {"attributes.DENOMINACION": 1,
                       "attributes.DISTRITO": 1,
                       "attributes.FECHAALTA": 1 }).sort([("attributes.DENOMINACION", 1), ("attributes.FECHAALTA", -1)]).limit(3)

    for c in cursor:
        pprint.pprint(c)
# fin-funcion

# Función que recibe por parámetro el nombre de la estación y
# devuelve sus coordenadas GPS
def getCoordenadasEstacion(est):
     print("\n\n*** Busqueda de coordenadas GPS por nombre de estación ***")

     cursor = col.find({"attributes.DENOMINACION" : est},
                       { "attributes.DENOMINACION": 1,
                         "geometry.x": 1,
                         "geometry.y": 1 })

     for c in cursor:
         pprint.pprint(c)
# fin-funcion


# Función que recorre todos los registros de la colección y
# muestra por pantalla para cada uno de ellos el valor del
# campo MODOINTERCAMBIADOR.
# Después calcula la media de estos valores y la muestra
def getAverage():
     print("\n\n*** Recorrido de todos los registros ***\n")
     cursor = col.find({}, {"attributes.DENOMINACION": 1, "attributes.MODOINTERCAMBIADOR": 1, "_id": 0, })

     for c in cursor:
         print(c)

     cursor = col.aggregate([
         {
             "$group": {
                 "_id": "null",
                 "Valor medio del campo MODOINTERCAMBIADOR": {
                     "$avg": "$attributes.MODOINTERCAMBIADOR"
                 }
             }
         }
     ])

     print("\n\n*** Cálculo de la media ***\n")

     for c in cursor:
         print(c)

# fin-funcion

if __name__ == "__main__":

    urlToOpen = "https://services5.arcgis.com/UxADft6QPcvFyDU1/arcgis/rest/services/Red_Metro/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"
    jsonFilename = "estaciones_metro_madrid.json"
    connData = "mongodb+srv://usuario1:chamaco77@prueba-k3hlc.mongodb.net/test?retryWrites=true&w=majority"

    # Abrimos url y volcamos contenido a fichero json local
    urlToJsonFile(urlToOpen,jsonFilename)

    # Conexion a MomgoDB Atlas
    client = pymongo.MongoClient(connData)

    # Obtenemos la BD
    db = client.METRO_MADRID

    # Obtenemos la coleccion
    col = db.estaciones

    # Insertamos sin bucles todos los datos del fichero json
    insertJsonDataInDB(jsonFilename)

    # Buscamos info para la estación con código "38"
    searchInfoByCodigoEstacion("38")

    # Buscamos las 3 primeras estaciones ordenadas por nombre
    # del distrito "03"
    searchDistritoTop3("03")

    # Obtenemos las coordenadas GPS de la estación "LAS ROSAS"
    getCoordenadasEstacion("LAS ROSAS")

    # Calculamos la media del campo MODOINTERCAMBIADOR
    # de todos los registros de la colección
    getAverage()
