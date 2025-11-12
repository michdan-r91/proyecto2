#INSTALAR
from pymongo import MongoClient
import redis
import json
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash


class GestorBaseDatos:
    def __init__(self):
        """Inicializa las conexiones con MongoDB y Redis."""
        self.mongo_cliente = MongoClient('mongodb://localhost:27017/')
        self.base = self.mongo_cliente['concurso_talentos']
        self.participantes = self.base['participantes']
        self.registro_votos = self.base['registro_votos']
        self.usuarios = self.base['usuarios']

        self.redis_cliente = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )

    # CARGA DE PARTICIPANTES (no sé si era algo así)

    def cargar_participantes_desde_json(self, ruta_json='concursantes.json'):
        """Carga los participantes desde un archivo JSON a MongoDB"""
        try:
            with open(ruta_json, 'r', encoding='utf-8') as archivo:
                datos = json.load(archivo)

            self.participantes.delete_many({})

            for p in datos:
                foto = p.get("foto", "")
                if not foto.startswith("http"):
                    foto = "https://cdn.pixabay.com/photo/2015/10/05/22/37/blank-profile-picture-973460_960_720.png" #REVISAR no sé si es algo así o cómo

                self.participantes.insert_one({
                    'id': p['id'],
                    'nombre': p['nombre'],
                    'categoria': p['categoria'],
                    'foto': foto,
                    'votos_acumulados': 0
                })

            return {"exito": True, "mensaje": f"{len(datos)} participantes cargados."}
        except Exception as e:
            return {"exito": False, "mensaje": f"Error al cargar participantes: {e}"}


    # AGREGAR NUEVO PARTICIPANTE

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
            return {"exito": True, "id": nuevo_id}
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}


    # REGISTRO DE VOTOS

    def registrar_voto(self, id_usuario, id_participante):
        """Registra un voto, actualiza MongoDB y Redis, y evita votos duplicados."""
        try:
            if self.registro_votos.find_one({'id_usuario': id_usuario, 'id_participante': id_participante}):
                return {"exito": False, "mensaje": "Ya votaste por este participante."}

            participante = self.participantes.find_one({'id': id_participante})
            if not participante:
                return {"exito": False, "mensaje": "Participante no encontrado."}

            nuevos_votos = participante['votos_acumulados'] + 1
            self.participantes.update_one({'id': id_participante}, {'$set': {'votos_acumulados': nuevos_votos}})

            # Actualizar o crear en Redis
            clave = f'participante:{id_participante}'
            datos_participante = {
                'id': participante['id'],
                'nombre': participante['nombre'],
                'categoria': participante['categoria'],
                'foto': participante['foto'],
                'votos_acumulados': nuevos_votos
            }
            self.redis_cliente.set(clave, json.dumps(datos_participante))
            self.redis_cliente.incr('total_votos')

            # Registrar el voto en MongoDB
            self.registro_votos.insert_one({
                'id_usuario': id_usuario,
                'id_participante': id_participante,
                'fecha_hora': datetime.now().isoformat()
            })

            return {"exito": True, "mensaje": f"Voto registrado para {participante['nombre']}."}
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}


    # CONSULTAS ADMIN Y PÚBLICAS

    def obtener_todos_los_participantes(self):
        """Devuelve todos los participantes (para módulo público)."""
        try:
            return list(self.participantes.find({}, {'_id': 0}))
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}

    def obtener_votos_en_tiempo_real(self):
        """Obtiene los votos en tiempo real desde Redis."""
        try:
            claves = self.redis_cliente.keys('participante:*')
            lista = [json.loads(self.redis_cliente.get(c)) for c in claves]
            lista.sort(key=lambda x: x['votos_acumulados'], reverse=True)
            total = int(self.redis_cliente.get('total_votos') or 0)
            return {'participantes': lista, 'total_votos': total}
        except Exception as e:
            return {'participantes': [], 'total_votos': 0, 'error': str(e)}

    def obtener_top3_participantes(self):
        """Top 3 de participantes con más votos."""
        try:
            return list(self.participantes.find(
                {}, {'_id': 0, 'id': 1, 'nombre': 1, 'categoria': 1, 'foto': 1, 'votos_acumulados': 1}
            ).sort('votos_acumulados', -1).limit(3))
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}

    def obtener_votos_por_categoria(self):
        """Total de votos agrupados por categoría."""
        try:
            pipeline = [
                {'$group': {'_id': '$categoria', 'total_votos': {'$sum': '$votos_acumulados'}}},
                {'$project': {'_id': 0, 'categoria': '$_id', 'total_votos': 1}},
                {'$sort': {'total_votos': -1}}
            ]
            return list(self.participantes.aggregate(pipeline))
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}

    def obtener_participantes_sin_votos(self):
        """Devuelve los participantes con 0 votos."""
        try:
            return list(self.participantes.find(
                {'votos_acumulados': 0},
                {'_id': 0, 'id': 1, 'nombre': 1, 'categoria': 1, 'foto': 1}
            ))
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}


    # USUARIOS Y LOGIN

    def verificar_usuario(self, usuario, contrasena):
        """Verifica credenciales de usuario."""
        try:
            u = self.usuarios.find_one({'username': usuario})
            if u and check_password_hash(u['password'], contrasena):
                return {'exito': True, 'rol': u['role'], 'id_usuario': str(u['_id'])}
            return {'exito': False, 'mensaje': 'Credenciales inválidas'}
        except Exception as e:
            return {'exito': False, 'mensaje': str(e)}

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


    # SINCRONIZACIÓN Y CIERRE

    def sincronizar_votos_con_redis(self):
        """Reconstruye el estado de Redis desde MongoDB."""
        try:
            participantes = self.participantes.find({'votos_acumulados': {'$gt': 0}})
            for p in participantes:
                clave = f'participante:{p["id"]}'
                self.redis_cliente.set(clave, json.dumps({
                    'id': p['id'],
                    'nombre': p['nombre'],
                    'categoria': p['categoria'],
                    'foto': p['foto'],
                    'votos_acumulados': p['votos_acumulados']
                }))
            total = self.participantes.aggregate([{'$group': {'_id': None, 'suma': {'$sum': '$votos_acumulados'}}}])
            total_votos = next(total, {'suma': 0})['suma']
            self.redis_cliente.set('total_votos', total_votos)
            return {"exito": True, "mensaje": "Sincronización completada."}
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}

    def cerrar_conexiones(self):
        """Cierra las conexiones a MongoDB y Redis."""
        try:
            self.mongo_cliente.close()
            self.redis_cliente.close()
            return {"exito": True, "mensaje": "Conexiones cerradas."}
        except Exception as e:
            return {"exito": False, "mensaje": str(e)}
