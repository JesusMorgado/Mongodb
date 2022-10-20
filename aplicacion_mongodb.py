from crudmysql import MySQL
from var import variables
from conf import variables as varmongo
from mongodb import Pymongo
from caja import Password

def cargar_estudiantes():
    obj_MySQL = MySQL(variables)
    obj_Pymongo = Pymongo(varmongo)

    #crear las consultas
    sql_estudiante = "select * from estudiantes"
    sql_kerdex = "select * from kardex"
    sql_usuario = "select * from usuarios"
    obj_MySQL.conectar_mysql()
    lista_estudiantes = obj_MySQL.consulta_sql(sql_estudiante)
    lista_kardex = obj_MySQL.consulta_sql(sql_kerdex)
    lista_usuarios = obj_MySQL.consulta_sql(sql_usuario)
    obj_MySQL.desconectar_mysql()

    #Insertar los datos en Mongo
    obj_Pymongo.conectar_mongodb()
    for est in lista_estudiantes:
        e = {
            "control": est[0],
            "nombre": est[1]
        }
        obj_Pymongo.insertar("estudiantes",e)
    for mat in lista_kardex:
        m = {
            "id_kardex": mat[0],
            "control": mat[1],
            "materia": mat[2],
            "calificacion": float(mat[3])
        }
        obj_Pymongo.insertar("kardex",m)
    for usu in lista_usuarios:
        u = {
            "id_usuario": usu[0],
            "control": usu[1],
            "clave": usu[2],
            "clave_cifrada": usu[3]
        }
        obj_Pymongo.insertar("usuarios",u)
    obj_Pymongo.desconectar_mongodb()

def insertar_estudiante():
    obj_Pymongo = Pymongo(varmongo)
    print("\n\n == INSERTAR ESTUDIANTES ==\n")
    ctrl = input("Dame el número de control: ")
    nombre = input("Dame el nombre del estudiante: ")

    cve = input("Dame la clave de acceso: ")
    obj_usuario = Password(longitud=len(cve), contraseña=cve)

    json_estudiante = {'control': ctrl, 'nombre':nombre} #f"INSERT INTO estudiantes VALUES('{ctrl}', '{nombre}');"
    json_usuario = {'idUsuario':100, 'control':ctrl, 'clave':cve, 'clave_cifrada':obj_usuario.contraseña_cifrada.decode()} #'INSERT INTO usuarios(control, clave, clave_cifrada) VALUES("{ctrl}", "{cve}", ' \
                 # f'"{obj_usuario.contrasena_cifrada.decode()}");'
    obj_Pymongo.conectar_mongodb()
    obj_Pymongo.insertar("estudiantes",json_estudiante)
    obj_Pymongo.insertar("usuarios", json_usuario)
    obj_Pymongo.desconectar_mongodb()
    print("Datos insertados Correctamente")

def actualizar_calificacion():
    obj_Pymongo = Pymongo(varmongo)

    print("\n\n == ACTUALIZAR CALIFICACION ==\n")
    ctrl = input("Dame el número de control: ")
    materia = input("Dame el nombre de la materia a actualizar: ")

    filtro_buscar_materia = {"control": ctrl, "materia": materia}
    obj_Pymongo.conectar_mongodb()
    respuesta = obj_Pymongo.consulta_mongodb("kerdex", filtro_buscar_materia)
    for reg in respuesta:
        print(reg)
    if respuesta:
        promedio = float(input("Dame el nuevo Promedio"))
        json_actualiza_prom = {"$set": {"calificacion": promedio}}
        resp = obj_Pymongo.actualizar("kardex",filtro_buscar_materia,json_actualiza_prom)
        if resp["status"]:
            print("Promedio actualizado")
        else:
            print("Ocurrio un error al actualizar")
    else:
        print(f"El estudiante con numero de control: {ctrl} o la materia: {materia} no se encontraron")
    obj_Pymongo.desconectar_mongodb()

def consulta_materia_estudiante():
    obj_PyMongo = Pymongo(varmongo)
    print(" == Consultar Materias por Estudiantes ==")
    ctr =input("Dame el numero de control")
    filtro = {"control":ctr}
    atributos_estudiantes = {"_id": 0, "nombre": 1}
    atributos_kardex = {"_id": 0, "materia": 1, "calificacion": 1}

    obj_PyMongo.conectar_mongodb()
    respuesta1 = obj_PyMongo.consulta_mongodb("estudiantes",filtro,atributos_estudiantes)
    respuesta2 = obj_PyMongo.consulta_mongodb("kardex",filtro,atributos_kardex)
    obj_PyMongo.desconectar_mongodb()

    if respuesta1["status"] and respuesta2["status"]:
        print(respuesta1["resultado"][0]["nombre"])
        for mat in respuesta2["resultado"]:
            print(mat["materia"], mat["calificacion"])


def consulta_gral_estudiantes():
    obj_Pymongo = Pymongo(varmongo)
    obj_Pymongo.conectar_mongodb()

    print("\n\n == CONSULTA GENERAL DE ESTUDIANTES ==\n")

    filtro = {}
    respuesta1 = obj_Pymongo.consulta_mongodb("estudiantes",filtro, {"_id": 0})

    for est in respuesta1["resultado"]:
        print()
        print("Estudiante --",est["nombre"])
        filtro ={"control": est["control"]}
        respuesta2 = obj_Pymongo.consulta_mongodb("kardex", filtro, {"_id": 0})
        print("---- Materias ----")
        for mat in respuesta2["resultado"]:
            print(mat["materia"], mat["calificacion"])
    obj_Pymongo.desconectar_mongodb()

def elimina_estudiante():
    obj_PyMongo = Pymongo(varmongo)
    obj_PyMongo.conectar_mongodb()

    print("\n\n == ELIMINAR UN ESTUDIANTE ==\n")
    ctrl = input("Dame el número de control: ")

    filtro = {"control": ctrl}

    respuesta1 = obj_PyMongo.eliminar('kardex', filtro)
    respuesta2 = obj_PyMongo.eliminar('usuarios', filtro)
    respuesta3 = obj_PyMongo.eliminar('estudiantes', filtro)

    obj_PyMongo.desconectar_mongodb()



def Menu():
    while True:
        print("============== Menú Principal ==============")
        print("1. Insertar un estudiante")
        print("2. Actualizar calificación")
        print("3. Consultar materias por estudiante")
        print("4. Consulta general de estudiantes")
        print("5. Eliminar a un estudiante")
        print("6. Salir")
        print("Elige la opción deseada")

        try:
            op = int(input(""))
        except Exception as error:
            print(error)
        else:
            if op == 1:
                insertar_estudiante()
            elif op == 2:
                pass
                actualizar_calificacion()
            elif op == 3:
                pass
                consulta_materia_estudiante()
            elif op == 4:
                pass
                consulta_gral_estudiantes()
            elif op == 5:
                pass
                elimina_estudiante()
            elif op == 6:
                break
            else:
                print("Opción incorrecta")


Menu()
#cargar_estudiantes()

