# Acerca del Servidor Web

El Servidor Web es la interfaz principal entre los usuarios y Aleph, el sistema de recolección de datos. Desde la Web los usuarios pueden ver datos en tiempo real, obtener registros de las bases de datos, visualizar los datos en forma rápida, generar visualizaciones complejas, crear reportes para imprimir o descargar, modificar y generar nuevos datos desde aplicaciones web, entre varias otras posibilidades.


## El espacio de nombres (namespace)

### Acerca del espacio de nombres

Aleph estructura los datos dentro de un espacio de nombres o _namespace_. Las distintas _entradas_ del espacio de nombres están identificadas jerárquicamente, como si fueran carpetas en un explorador. 

Por ejemplo, en la entrada tonutti.planta.produccion.sala\_duros.tinas.plc se encuentran los datos correspondientes a ese equipo. Cada nivel en el espacio de nombres se separa con un punto ".", y no se utilizan mayúsculas o espacios.

Una entrada en el espacio de nombres contiene varios campos y varios registros.

```{admonition} NOTA
:class: note
El espacio de nombres es análogo a una base de datos ordenada jerárquicamente. Las entradas son tablas, los campos las columnas y los registros las filas.
```

```{admonition} NOTA
:class: note
Cada registro no tiene que contener todos los campos necesariamente.
```

```{admonition} NOTA
:class: note
El Explorador (/explorer) permite visualizar el espacio de los nombres y los datos en cada entrada.
```

### Namespace extras

Para facilitar la visualización en el explorador, se pueden modificar los nombres de los campos y agregar mensajes de ayuda (_tooltips_). Esto se puede hacer desde el menú Más > Namespace Extras en el explorador, habiendo antes seleccionado la entrada y el campo a modificar. Se pueden utilizar puntos "." para ordenar los campos jerárquicamente igual que en el espacio de nombres.

```{admonition} CUIDADO
:class: warning
Modificar el Namespace Extra no altera en absoluto la base de datos de Aleph o el espacio de nombres. Las modificaciones en Namespace Extras sólo son visibles en el explorador, pero los nombres de los campos siguen siendo los originales.
```


## Usuarios

### Tipos de usuarios

Distintos tipos de usuarios pueden interactuar con el servidor web:
- _Admins_, que pueden modificar la configuración y los usuarios en el servidor
- _Superusuarios_, que pueden acceder a todo el espacio de nombres y ver datos en tiempo real (se conectan directamente al WebSocket del Servidor MQTT)
- Usuarios comunes, que pueden acceder sólo a una parte del espacio de nombres (configurado por un _admin_) y no pueden ver datos en tiempo real.

### Agregar, modificar o eliminar un usuario

Sólo los admins pueden agregar, modificar o eliminar un usuario desde la interfaz de administrador (/admin).

```{admonition} RECUPERAR UNA CONTRASEÑA
:class: warning
En caso de que un usuario olvide una contraseña, ésta sólo puede ser restablecida por un administrador.
```


## Contenido

El Servidor Web contiene tres tipos principales de recursos:
- _Dashboards_. Pantallas interactivas para visualizar datos.
- _Reportes_. Otra forma de visualizar datos, que permite imprimirlos fácilmente.
- _Archivos_. Generados en forma dinámica, permiten obtener los datos directamente en CSV, XLS u otro formato.

Además el servidor cuanta con:
- _Explorador_. Permite visualizar los datos rápidamente.
- _API_. Permite obtener datos, modificar el namespace, entre otros.

```{admonition} NOTA
:class: important
Todos los recursos y links relevantes están disponibles en la página principal que ve cada usuario luego de iniciar sesión.
```

```{admonition} AVISO
:class: warning
Cada recurso está relacionado con una entrada en el espacio de los nombres. Sólo se puede
```

