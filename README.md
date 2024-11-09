# PIPELINE

PIPELINE es una herramienta diseñada para procesar grandes volúmenes de datos de manera eficiente, permitiendo la transformación y exportación de información en formatos como Parquet, CSV y JSON.

## Estructura del Proyecto

El proyecto está organizado de la siguiente manera:

- **`config/`**: Archivos de configuración.
- **`data/`**:
  - `raw/`: Datos sin procesar.
  - `final/`: Resultados finales.
- **`doc/`**: Documentación del proyecto.
- **`logs/`**: Archivos de registro generados durante la ejecución.
- **`src/`**: Código fuente principal.
- **`tests/`**: Pruebas unitarias y de integración.


## Documentación Adicional

Para obtener más detalles sobre el funcionamiento y la arquitectura del proyecto, puedes descargar la documentación completa aquí:

[Descargar Documentación del Pipeline de Procesamiento de Datos](https://github.com/Danielmc09/PIPELINE/raw/main/doc/Documentaci%C3%B3n%20del%20Pipeline%20de%20Procesamiento%20de%20Datos.docx)


## Tabla de Contenidos

- [Requisitos Previos](#requisitos-previos)
- [Instalación y Ejecución](#instalación-y-ejecución)
- [Configuración](#configuración)
- [Uso](#uso)
- [Características](#características)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)
- [Contacto](#contacto)

## Requisitos Previos

Antes de instalar y ejecutar PIPELINE, asegúrate de tener instalados los siguientes componentes:

- **Docker:** Puedes descargarlo e instalarlo desde [docker.com](https://www.docker.com/get-started).
- **Docker Compose:** Generalmente incluido con Docker Desktop. Verifica su instalación ejecutando `docker-compose --version` en tu terminal.

## Instalación y Ejecución

Sigue estos pasos para clonar el repositorio y ejecutar el proyecto utilizando Docker:

1. **Clona el repositorio:**

   ```
   git clone https://github.com/Danielmc09/PIPELINE.git
   ```

2. **Navega al directorio del proyecto:**

   ```
   cd PIPELINE
   ```

3. **Construye y levanta los servicios con Docker Compose:**

   ```
   docker-compose up --build
   ```

Este comando descargará las imágenes necesarias, construirá el contenedor y ejecutará la aplicación.

## Configuración

Antes de ejecutar PIPELINE, es necesario configurar ciertos parámetros:

1. **Archivo de configuración:** En el directorio `config`, encontrarás un archivo `config.yaml`. Edita este archivo para establecer las rutas de entrada y salida, así como otros parámetros necesarios.

2. **Variables de entorno:** Si tu proyecto requiere variables de entorno específicas, defínelas en un archivo `.env` en la raíz del proyecto.

## Uso

Una vez que el contenedor esté en ejecución, la aplicación procesará los datos según la configuración especificada.

Para detener la aplicación y eliminar los contenedores, presiona `Ctrl+C` en la terminal donde se está ejecutando o utiliza:

```
docker-compose down
```

## Características

- **Procesamiento en paralelo:** Utiliza múltiples núcleos para acelerar el procesamiento de datos.
- **División de archivos:** Maneja grandes volúmenes de datos dividiéndolos en bloques más pequeños.
- **Soporte de múltiples formatos:** Genera archivos de salida en formatos CSV y JSON según las necesidades del usuario.

## Contribuciones

Las contribuciones son bienvenidas. Para contribuir:

1. Haz un fork del repositorio.
2. Crea una nueva rama (``` git checkout -b feature/nueva-funcionalidad ```).
3. Realiza tus cambios y haz commit (``` git commit -m 'Añadir nueva funcionalidad' ```).
4. Haz push a la rama (``` git push origin feature/nueva-funcionalidad ```).
5. Abre un Pull Request.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo [LICENSE](LICENSE) para más detalles.

## Contacto

Para consultas o sugerencias, contacta a [danielmc0911@gmail.com] o abre un issue en el repositorio.

Autor: <a href="https://www.linkedin.com/in/danielmendietadeveloper/">Angel Daniel Menideta Castillo</a> © 2024
