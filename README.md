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

- **NiFi**: Se utiliza esta herramienta de ingestión de datos, pues cuenta directamente con un procesador con el que obtener los tweets en función del hashtag (GetTwitter), y también porque la podemos utilizar como ETL, para dividir los tweets en palabras y extraerlas a fin de asignarlas al campo correspondiente de la base de datos. Igualmente, existe un procesador PublishKafkaRecord que nos sirve para introducir en la cola de Kafka los tweets de usuarios de manera eficiente y sencilla.
- **PostgreSQL**: Base de datos relacional donde se almacenan los datos de viviendas disponibles. Hemos escogido una base de datos SQL, porque la estructura de los datos de viviendas será siempre la misma, ya que el tweet tipo no varía, por lo que se puede definir un esquema de la tabla de viviendas.
- **Kafka**: Hemos incluido esta herramienta de mensajería para gestionar el volumen de datos provenientes de NiFi (tweets de usuarios), con el fin de que Spark consuma de la cola de Kafka en función de sus necesidades y para evitar cuellos de botella, asegurando así el correcto flujo de datos desde nuestro software de ingestión hacia el procesamiento en streaming.
- **Spark**: Se ha decidido utilizar esta tecnología Big Data para el procesamiento de los datos de tweets, ya que permite un procesamiento en tiempo real (Spark Streaming) y es perfectamente escalable al poder procesar en distribuido, de cara al proyecto real, donde el nivel de tweets por minuto será mayor y necesitemos operar en varias instancias.
- **Python**: Como lenguaje donde programar el algoritmo de elección de viviendas para los usuarios, así como la conexión con la base de datos y la API de Twitter, ya que es el que más ampliamente se ha visto en clase y con el que más cómodos nos sentimos programando.
- **Docker**: Se han implementado cinco Dockers: para NiFi, para la base de datos y para el gestor, para Kafka y para ejecutar el algoritmo. Se ha decidido así ya que era una forma de realizar el despliegue y la configuración de manera automática. Así además, este sistema se convierte en escalable, ya que ante un aumento en el volumen de tweets se pueden desplegar más contenedores de la parte que se necesite escalar en ese momento.
- **Google Cloud**: Hemos elegido Google Cloud para tener ejecutando constantemente tanto NiFi, como la base de datos y el gestor de bases de datos (PGAdmin), así como Kafka y el algoritmo (Spark-Python). Su elección es debido a que ha sido el que se ha visto en clase y con el cual nos sentíamos cómodos para trabajar.

## Esquema de la Base de Datos

<img src="https://github.com/cadoca1/dataproject2/blob/main/Esquema_BD.png" width="300"/>

## Esquema del GitHub

- [**Docker**](https://github.com/cadoca1/dataproject2/tree/main/docker): Carpeta con los Dockerfile y los Docker-compose para levantar en Docker todos los procesos. Estructurados por servicio.
- [**NiFi**](): Template de todo el proceso de ingestión de NiFi, desde Twitter hasta la base de datos PostgreSQL y hasta el broker y topic de Kafka.
- [**Algoritmo**](https://github.com/cadoca1/dataproject2/blob/main/algoritmo.py): Fichero Python del proceso Spark-Python que realiza todo el proceso de asignar una vivienda disponible a cada usuario.

## Anexos

[Twitter](https://twitter.com/Carlos11284606/with_replies)

[Trello](https://trello.com/b/eVk7yKLI/dataproject2)

[PGAdmin](http://35.238.29.211:5050)
  - pgadmin4@pgadmin.org
  - admin

## Contribuidores
- Amparo Botella
- Carlos Donoso
- Jordi Oltra
- Juan Luis Hernández
- María Carbonell
