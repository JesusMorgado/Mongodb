# clase para conectarnos a mongodb
import pymongo
from crudmysql import MySQL
from conf import variables
from var import variables as varsql
class Pymongo:
    def __init__(self,variables): # host = 'localhost', db='opensource', port = '27017', timeout=1000, user='', password=''
        self.MONGO_DATABASE = variables["bd"]
        self.MONGO_URL = 'mongodb://' + variables["host"] + ':' + variables["port"]
        self.MONGO_CLIENT = None
        self.MONGO_RESPUESTA = None
        self.MONGO_TIMEOUT = variables["timeout"]

    def conectar_mongodb(self):
        try:
            self.MONGO_CLIENT = pymongo.MongoClient(self.MONGO_URL, serverSelectionTimeOutMS=self.MONGO_TIMEOUT)  # Conectado
        except Exception as error:
            print("Error" ,error)
        else:
            pass
            #print("Conexion al servidor de Mongodb realizado")
        #finally:

    def desconectar_mongodb(self):
        if self.MONGO_CLIENT:
            self.MONGO_CLIENT.close()

    def consulta_mongodb(self,tabla,filtro,atributos = {"_id":0}):
        response = {"status": False, "resultado":[]}
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self. MONGO_DATABASE][tabla].find(filtro,atributos)
        if self.MONGO_RESPUESTA:
            response["status"] = True
            for reg in self.MONGO_RESPUESTA:

                response["resultado"].append(reg)

           #for reg in self.MONGO_RESPUESTA:
            #print(reg)
        return response

    def insertar(self,tabla,documento):
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self. MONGO_DATABASE][tabla].insert_one(documento)
        if self.MONGO_RESPUESTA:
            return self.MONGO_RESPUESTA
        else:
            return None

    # Actualizar documentos en las colecciones
    def actualizar(self,tabla,filtro,nuevos_valores):
        response = {"status":False}
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][tabla].update_many(filtro,nuevos_valores)
        if self.MONGO_RESPUESTA:
            response["status"] = True
            #return self.MONGO_RESPUESTA
        #else:
        return response

    def eliminar(self, tabla, filtro):
        response = {"status": False}
        self.MONGO_RESPUESTA = self.MONGO_CLIENT[self.MONGO_DATABASE][tabla].delete_one(filtro)
        if self.MONGO_RESPUESTA:
            response = {"status": True}

        return response





#obj_mongo =Pymongo(variables)
#obj_mongo.conectar_mongodb()
#obj_mongo.consulta_mongodb('estudiantes')
#obj_mongo.insertar_estudiante(alumno)
#obj_mongo.desconectar_mongodb()
