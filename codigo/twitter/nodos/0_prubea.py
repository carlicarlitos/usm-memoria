from neo4jrestclient.client import GraphDatabase

gdb = GraphDatabase("http://localhost:7474/db/data/", username="neo4j", password="memoria")

#consultar si existe nodo
#consulta_nodo = f"""MATCH (n) WHERE (n.id = {user_id}) RETURN n"""


#consultar si existe amistad
#consultar_amistad = f"""RETURN EXISTS( (:User {{ id: {me_sigue} }})-[:Follows]->(:User {{ id: {yo} }} ) )"""

#crear relacion de seguidos
#crear_amistad = f"""MATCH (n),(m) WHERE (n.id = {me_sigue}) AND (m.id = {yo}) CREATE (n) -[FOLLOWS] -> (m)"""

#crear nodos
#crear_nodo = f"""CREATE ({n_user}:User {{ id : {user_id}, origen: {origen} }})"""

#CREAR TODOS LOS NODOS DE LA LISTA

"""user_id = 1111
origen = "lista"
n_aux = "n"+str(user_id)
crear_nodo = crear_nodo_aux.format(n_aux)
print(crear_nodo)
"""
users = [12,1,3311036150]
yo = 856850515
for user in users:
    me_sigue = user
    consultar_amistad = f"""RETURN EXISTS( (:User {{ id: {me_sigue} }})-[:Follows]->(:User {{ id: {yo} }} ) )"""
    result = gdb.query(consultar_amistad, data_contents=True)
    nodo_obtenido = result.rows[0][0]
    print(nodo_obtenido)
    if nodo_obtenido is False:
        print("no existe")
    else:
        print("existe")
x;










