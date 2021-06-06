# Admin

Los administradores pueden agregar, modificar y eliminar usuarios y editar otras configuraciones del Servidor Web. Todo lo pueden hacer desde la interfaz /admin.


## Users

La edición de usuarios es tarea exclusiva de los administradores.


### Tipos de usuarios

- _Admins_, que pueden modificar la configuración y los usuarios en el servidor
- _Superusuarios_, que pueden acceder a todo el espacio de nombres y ver datos en tiempo real (se conectan directamente al WebSocket del Servidor MQTT)
- Usuarios comunes, que pueden acceder sólo a una parte del espacio de nombres (configurado por un _admin_) y no pueden ver datos en tiempo real.


### Restablecer una contraseña

Para restablecer una contraseña, abrir la interfaz de administrador (/admin), seleccionar _Users_, seleccionar el usuario a modificar y seleccionar _reset password_.


## API Tokens

Los administradores pueden asignar y eliminar tokens para el acceso de los distintos usuarios a la API. Leer la documentación de la API para más detalles.


## Activity Log

Permite visualizar la actividad de los distintos usuarios.


## Access Control

La configuración en Access Control determina a qué sección del espacio de nombres puede acceder cada usuario, es decir, los **Permisos**.

Se puede utilizar el símbolo "#" para generalizar la entrada correspondiente al espacio de nombres. Por ejemplo, si un usuario tiene acceso a "tonutti.#", significa que tendrá acceso a todo el espacio de nombres debajo de "tonutti".

```{admonition} NOTA
:class: note
Los permisos definidos en Access Control sólo afectan a los usuarios comunes. Los superusuarios pueden acceder a cualquier entrada del espacio de nombres.
```

```{admonition} CUIDADO
:class: warning
Cada recurso del Servidor Web (dashboards, reportes, archivos y otros) está asociado a una entrada en el espacio de los nombres, de modo que en access control también se determinan los permisos de acceso a los distintos recursos.
```

```{admonition} CUIDADO
:class: warning
Por defecto, un usuario tiene bloqueado por completo el espacio de nombres. 
```


## Namespace Extras

Los datos de Namespace Extras pueden ser modificados directamente desde la página del administrador, aunque es más fácil la edición desde el explorador. Ver más en "Acerca del Servidor Web".

