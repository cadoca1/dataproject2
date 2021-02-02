# Dataproject 2 - Karimun Jawa

<img src="https://github.com/juanluishg/dataproject1/blob/main/WhatsApp%20Image%202020-12-11%20at%2022.35.04.jpeg" width=50%/>

## Video Explicativo

[Video Youtube]()

## Funcionamiento

1. Se introduce un tweet nuevo de un usuario que busca casa, incluyendo su nombre, salario, hobbies y número de miembros que conforman su familia, así como un hashtag en concreto para localizarlo: #mdaedem.
2. Se introducen ofertas de casas disponibles mediante tweets utilizando el mismo hashtag e incluyendo su precio al mes, la ciudad en la que se encuentran, el número de habitaciones y un código único de identificación.
3. Respondemos al tweet del usuario, asignándole su casa ideal acorde a sus preferencias individuales (con su código de identificación), y adjuntando un enlace al tweet de la casa en concreto, así como una foto de la ciudad en la que se encuentra la misma.

## Proceso

1. Mediante NiFi obtenemos los tweets que nos interesan (procesador GetTwitter), tanto de usuarios como de casas disponibles, filtrando por el hashtag "#mdaedem".
2. Diferenciamos dos clases de tweets en función de su estructura: los de viviendas los enviamos a la base de datos en PostgreSQL (enviamos desde NiFi los campos que nos interesan a la tabla creada en la base de datos); y los de usuarios a la cola de Kafka.
3. Spark consume de Kafka los tweets de usuarios e igualmente lee de la base datos la información de las viviendas y los procesa mediante un algoritmo desarrollado con PySpark (Python) para realizar la asignación anteriormente comentada.
4. Posteriormente, se conecta con la API de Twitter y publicamos las respuestas a los tweets de usuarios.

Nota: Hemos determinado un número máximo de 2 casas para asignar a cada usuario, así como un máximo de 4 usuarios a los que le puede llegar una oferta de la misma vivienda.

## Arquitectura

<img src=""/>

### Justificación 

## Esquema de la Base de Datos

<img src="https://github.com/cadoca1/dataproject2/blob/main/Esquema_BD.png" width="300"/>

## Esquema del Github

## Anexos

## Contribuidores
- Amparo Botella
- Carlos Donoso
- Jordi Oltra
- Juan Luis Hernández
- María Carbonell
