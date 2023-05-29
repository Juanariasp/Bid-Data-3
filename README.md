# **Parcial_Big_Data**

<div> 
<img src="https://res-5.cloudinary.com/crunchbase-production/image/upload/c_lpad,h_256,w_256,f_auto,q_auto:eco/v1455514364/pim02bzqvgz0hibsra41.png" align="right"><FONT FACE="times new roman" SIZE=5>
<br>
<i><b>Docente:</b></i> Camilo Rodriguez
<br>
<i><b>Asignatura:</b></i> Big - Data 
<br>
<i><b>Tema:</b></i> Tercer Parcial
<br>
<i><b>Estudiantes: </b> </i> 
<li>Christian David Jimenez</li>
<li>Juan Camilo Hernandez</li>
<li>Juan Esteban Arias </li>
</FONT>
</div>

<FONT FACE='times new roman'>
<h1><b>Introduccion</b></h1>
<p align='justify'>
<FONT SIZE = 4>
El parcial consta de cuatro partes:<br><br>
<li>Crear una función lambda con Zappa que descargue la primera página de resultados del sitio web Finca Raiz para la venta de casas en el sector de Chapinero. Esta lambda debe ejecutarse todos los lunes a las 9 am. La página HTML se debe guardar en un bucket S3 con la ruta s3://landing-casas-xxx/yyyy-mm-dd.html.<br><br>
<li>Al llegar la página web al bucket S3, se debe disparar una segunda lambda que procese el archivo utilizando Beautiful Soup y extraiga la información de cada casa. Se debe crear un archivo CSV en s3://casas-final-xxx/yyyy-mm-dd.csv con la siguiente estructura de columnas: FechaDescarga, Barrio, Valor, NumHabitaciones, NumBanos, mts2. <br><br>
<li>Se deben utilizar pruebas unitarias con pytest, incluyendo un mínimo de tres pruebas. Para probar la función de descarga, se debe utilizar un mock.<br><br>
<li>Se debe crear un pipeline de despliegue continuo con GitHubActions que conste de tres etapas: revisión de código limpio con flake8, ejecución de pruebas unitarias y despliegue automático en AWS.
</p>

<h1><b>Archivos</b></h1>
<p align='justify'>
<FONT SIZE = 4>
El repositorio consta de 7 archivos:<br><br>
<ol>
<li><b>requirements.txt: </b>Es utilizado para especificar las dependencias requeridas por el proyecto. Este archivo lista todas las librerías Python que el proyecto necesita para ser ejecutado.<br><br>
Es útil para mantener la compatibilidad entre los diferentes entornos de desarrollo, ya que los mismos paquetes y versiones serán instalados en todas las máquinas que ejecuten el proyecto. Esto ayuda a evitar errores y conflictos en el entorno de producción. <br><br>
<li><b>zappa_settings.json: </b>Archivo utilizado en el despliegue de las funciones Lambda utilizando Zappa. Este archivo se utiliza para configurar los ajustes específicos de las funciones y los buckets de la infraestructura de AWS.<br><br>
<li><b>leer_archivo.py: </b>Este archivo descarga el contenido de la página web y lo guarda en un bucket creado en AWS S3. Además, está programada para que se ejecute de forma automática todos los días a las 9:00 AM. De esta manera, se puede mantener actualizado el contenido de la página web sin la necesidad de hacerlo manualmente.<br><br>
<li><b>recibir_archivo.py: </b>Una vez que el archivo de la página web es recibido en el bucket, este archivo de Python se ejecuta de forma automática. Su objetivo es procesar el contenido del archivo HTML utilizando la librería Beautiful Soup y extraer la información relevante de cada casa. Posteriormente, esta información se almacena en un archivo CSV en el bucket de S3. De esta manera, se puede obtener la información de cada casa de forma estructurada y utilizarla para análisis y otros propósitos.<br><br>
<li><b>test_leer_archivo.py: </b>En este archivo se encuentran las pruebas unitarias correspondientes para comprobar el correcto funcionamiento de la aplicación. Las pruebas unitarias son esenciales en el proceso de desarrollo de software, ya que permiten verificar si cada componente o unidad de la aplicación está funcionando de forma adecuada y sin errores. <br><br>
<li><b>.gitignore: </b>Especifica los archivos y directorios que deben ser ignorados por Git durante el seguimiento de cambios y la sincronización con el repositorio remoto.<br><br>
<li><b>flujo.yml: </b>Este archivo determina los flujos de trabajo que se ejecutan en respuesta a ciertos eventos, como la confirmación de código, la creación de solicitudes de extracción o el cambio de ramas. El archivo "workflows" se escribe en formato YAML y se utiliza para definir los pasos que deben seguirse en cada flujo de trabajo. Estos pasos pueden incluir la ejecución de pruebas, la compilación del código y la implementación en el entorno de producción.<br><br>
</ol>
</p>
