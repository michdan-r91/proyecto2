from pymongo import MongoClient
import redis
import json, os, glob
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash


class GestorBaseDatos:
    """Gestor de conexión y operaciones entre MongoDB y Redis"""

    def __init__(self):
        # Conexión a MongoDB
        self.mongo_cliente = MongoClient('mongodb://localhost:27017/')
        self.base = self.mongo_cliente['concurso_talentos']
        self.participantes = self.base['participantes']
        self.registro_votos = self.base['registro_votos']
        self.usuarios = self.base['usuarios']

        # Conexión a Redis
        self.redis_cliente = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


    # Carga de participantes

    def cargar_participantes_desde_json(self, ruta_json=None):
        """Carga los participantes desde un archivo JSON a MongoDB."""
        try:
            if ruta_json is None:
                archivos = glob.glob("*.json")
                if not archivos:
                    print(" No se encontró ningún archivo JSON.")
                    return False
                ruta_json = archivos[0]
                print(f" Cargando desde: {ruta_json}")

            with open(ruta_json, 'r', encoding='utf-8') as archivo:
                datos = json.load(archivo)

            self.participantes.delete_many({})
            for p in datos:
                foto = p.get("foto", "")
                if not foto.startswith("http"):
                    foto = "https://cdn.pixabay.com/photo/2015/10/05/22/37/blank-profile-picture-973460_960_720.png"
                self.participantes.insert_one({
                    'id': p['id'],
                    'nombre': p['nombre'],
                    'categoria': p['categoria'],
                    'foto': foto,
                    'votos_acumulados': 0
                })

            print(f" {len(datos)} participantes cargados correctamente.")
            return True
        except Exception as e:
            print(f" Error al cargar participantes: {e}")
            return False


    # Agregar nuevo participante

    def agregar_participante(self, nombre, categoria, foto=None):
        """Agrega un nuevo participante con votos iniciales en 0."""
        try:
            ultimo = self.participantes.find_one(sort=[('id', -1)])
            nuevo_id = (ultimo['id'] + 1) if ultimo else 1
            if not foto or not foto.startswith("http"):
                foto = "https://cdn.pixabay.com/photo/2015/10/05/22/37/blank-profile-picture-973460_960_720.png"

            self.participantes.insert_one({
                'id': nuevo_id,
                'nombre': nombre,
                'categoria': categoria,
                'foto': foto,
                'votos_acumulados': 0
            })
            print(f" Participante '{nombre}' agregado con ID {nuevo_id}")
            return nuevo_id
        except Exception as e:
            print(f" Error al agregar participante: {e}")
            return None


    # Registro de votos

    def registrar_voto(self, id_usuario, id_participante):
        """Registra un voto, actualiza MongoDB y Redis, y evita duplicados."""
        try:
            # Verifica voto duplicado
            if self.registro_votos.find_one({'id_usuario': id_usuario, 'id_participante': id_participante}):
                return False, "Ya votaste por este participante."

            participante = self.participantes.find_one({'id': id_participante})
            if not participante:
                return False, "Participante no encontrado."

            # Actualiza votos en MongoDB
            nuevos_votos = participante['votos_acumulados'] + 1
            self.participantes.update_one({'id': id_participante}, {'$set': {'votos_acumulados': nuevos_votos}})

            # Actualiza o crea en Redis
            clave = f'participante:{id_participante}'
            datos = {
                'id': participante['id'],
                'nombre': participante['nombre'],
                'categoria': participante['categoria'],
                'foto': participante['foto'],
                'votos_acumulados': nuevos_votos
            }
            self.redis_cliente.set(clave, json.dumps(datos))
            self.redis_cliente.incr('total_votos')

            # Registra el voto en MongoDB
            self.registro_votos.insert_one({
                'id_usuario': id_usuario,
                'id_participante': id_participante,
                'fecha_hora': datetime.now().isoformat()
            })

            return True, f"Voto registrado para {participante['nombre']}."
        except Exception as e:
            print(f" Error al registrar voto: {e}")
            return False, f"Error del servidor: {str(e)}"

    # Consltas del admin y publico

    def obtener_todos_los_participantes(self):
        """Lista completa de participantes (módulo público)."""
        try:
            return list(self.participantes.find({}, {'_id': 0}))
        except:
            return []

    def obtener_votos_en_tiempo_real(self):
        """Obtiene los votos en tiempo real desde Redis."""
        try:
            claves = self.redis_cliente.keys('participante:*')
            lista = [json.loads(self.redis_cliente.get(c)) for c in claves]
            lista.sort(key=lambda x: x['votos_acumulados'], reverse=True)
            total = int(self.redis_cliente.get('total_votos') or 0)
            return {'participantes': lista, 'total_votos': total}
        except:
            return {'participantes': [], 'total_votos': 0}

    def obtener_top3_participantes(self):
        """Top 3 de participantes con más votos."""
        return list(self.participantes.find(
            {}, {'_id': 0, 'id': 1, 'nombre': 1, 'categoria': 1, 'foto': 1, 'votos_acumulados': 1}
        ).sort('votos_acumulados', -1).limit(3))

    def obtener_votos_por_categoria(self):
        """Votos totales por categoría."""
        pipeline = [
            {'$group': {'_id': '$categoria', 'total_votos': {'$sum': '$votos_acumulados'}}},
            {'$project': {'_id': 0, 'categoria': '$_id', 'total_votos': 1}},
            {'$sort': {'total_votos': -1}}
        ]
        return list(self.participantes.aggregate(pipeline))

    def obtener_participantes_sin_votos(self):
        """Lista de participantes con 0 votos."""
        return list(self.participantes.find(
            {'votos_acumulados': 0}, {'_id': 0, 'id': 1, 'nombre': 1, 'categoria': 1, 'foto': 1}
        ))


    # Gstion de usuarios

    def verificar_usuario(self, usuario, contrasena):
        """Verifica credenciales y devuelve rol."""
        try:
            u = self.usuarios.find_one({'username': usuario})
            if u and check_password_hash(u['password'], contrasena):
                return {'exito': True, 'rol': u['role'], 'id_usuario': str(u['_id'])}
            return {'exito': False, 'mensaje': 'Credenciales inválidas'}
        except:
            return {'exito': False, 'mensaje': 'Error del servidor'}

    def crear_usuario_publico(self, usuario, contrasena):
        """Crea un usuario público con contraseña cifrada."""
        if self.usuarios.find_one({'username': usuario}):
            return {'exito': False, 'mensaje': 'Usuario ya existe'}
        self.usuarios.insert_one({
            'username': usuario,
            'password': generate_password_hash(contrasena),
            'role': 'publico'
        })
        return {'exito': True, 'mensaje': 'Usuario público creado'}

    def crear_usuario_admin(self, usuario, contrasena):
        """Crea un usuario administrador con contraseña cifrada."""
        if self.usuarios.find_one({'username': usuario}):
            return {'exito': False, 'mensaje': 'Usuario ya existe'}
        self.usuarios.insert_one({
            'username': usuario,
            'password': generate_password_hash(contrasena),
            'role': 'admin'
        })
        return {'exito': True, 'mensaje': 'Administrador creado'}
