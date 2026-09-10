# Informe: TP Nivelador — Sistema de Lotería Distribuido

## Introducción

Este informe describe el protocolo de comunicación implementado entre las agencias
(clientes) y el servidor central de lotería, así como los mecanismos utilizados
para sincronizar la ejecución concurrente en ambos extremos del sistema.

El cliente está desarrollado en Go y el servidor en Python. Cada agencia corre
como un contenedor cliente independiente que envía sus apuestas al servidor por
una conexión TCP propia.

## Protocolo de comunicación

### Formato de los mensajes

Todo mensaje sobre el socket sigue el mismo framing:

```
[1 byte: tipo][4 bytes big-endian: longitud del payload][payload]
```

El campo de longitud permite leer exactamente `N` bytes de payload sin depender
de delimitadores dentro de los datos, y evita el problema de los *short
reads/writes* de TCP: tanto el envío como la recepción están implementados con
funciones auxiliares (`SendAll`/`RecvAll` en el cliente, `send_all`/`recv_all`
en el servidor) que reintentan hasta completar la cantidad de bytes esperada,
en lugar de asumir que una sola llamada a `send`/`recv` transmite todo el
mensaje.

### Tipos de mensaje

| Tipo         | Valor | Dirección          | Contenido                                   |
|--------------|-------|---------------------|----------------------------------------------|
| `BET_BATCH`  | 0x01  | cliente → servidor  | Lote de N apuestas                            |
| `BATCH_ACK`  | 0x02  | servidor → cliente  | 1 byte de estado (OK / ERROR)                 |
| `FINISHED`   | 0x03  | cliente → servidor  | ID de agencia, no hay más apuestas por enviar |
| `WINNERS`    | 0x04  | servidor → cliente  | Lista de documentos ganadores de esa agencia  |

### Codificación de `BET_BATCH`

```
[2 bytes: cantidad de apuestas]
  por cada apuesta:
    [4 bytes: agency_id]
    [1 byte: longitud del nombre][nombre]
    [1 byte: longitud del apellido][apellido]
    [4 bytes: documento]
    [10 bytes: fecha de nacimiento, "YYYY-MM-DD"]
    [4 bytes: número apostado]
```

Se usan campos de longitud variable (1 byte) para nombre y apellido en lugar de
un tamaño fijo, para no desperdiciar ancho de banda con padding en campos de
longitud arbitraria, mientras que los campos de tamaño conocido (documento,
número, fecha) se codifican con tamaño fijo.

### Envío por lotes (batching)

El cliente no manda una apuesta por mensaje: acumula hasta `BATCH_SIZE`
apuestas (configurable por variable de entorno) y recién ahí arma y envía el
payload. Esto reduce drásticamente la cantidad de round-trips y de syscalls
frente a enviar apuesta por apuesta, a costa de introducir un límite de tamaño
de mensaje: `BATCH_SIZE` determina cuánta memoria e latencia adicional se
tolera antes de que una apuesta "espere" a ser enviada.

Cada batch se confirma individualmente con `BATCH_ACK` antes de continuar con
el siguiente, lo que simplifica el manejo de errores: si el servidor rechaza
un batch, el cliente lo sabe antes de haber comprometido más apuestas.

### Cierre del intercambio y consulta de ganadores

Cuando el cliente terminó de leer su archivo de entrada, envía `FINISHED` con
su `agency_id`. El servidor no responde inmediatamente: cada agencia se
bloquea hasta que se cumple un *quorum* mínimo de agencias finalizadas
(`AGENCY_QUORUM_MIN`), momento en el cual el servidor realiza el sorteo y
responde a **cada agencia únicamente con sus propios documentos ganadores**
(`WINNERS`), nunca con la lista completa de todos los participantes. Esto
evita filtrar información de otras agencias.

## Mecanismos de sincronización

### Servidor (Python, un thread por conexión)

El servidor acepta conexiones de forma serial en el thread principal
(`accept()`), pero delega el manejo de cada cliente a un thread propio
(`daemon=True`), permitiendo que múltiples agencias envíen sus apuestas en
paralelo.

Dos recursos compartidos requieren sincronización explícita:

- **El almacenamiento de apuestas (`bets.csv`)**: protegido por un
  `threading.Lock` (`_storage_lock`). Todo acceso de lectura o escritura al
  archivo se hace dentro de esta sección crítica, evitando que dos threads
  entrelacen una escritura a mitad de línea o lean un archivo a medio
  escribir.

- **El quorum de agencias finalizadas**: coordinado con una
  `threading.Condition` (`_quorum_cv`) y un `set` compartido
  (`_finished_agencies`). Cada thread que recibe `FINISHED` agrega su
  `agency_id` al set y espera (`wait_for`) con un predicado que se re-evalúa
  en cada notificación, hasta que se alcanza el mínimo de agencias
  configurado. El thread que efectivamente completa el quorum despierta a
  todos los demás (`notify_all`). Usar un predicado explícito en `wait_for`
  evita el problema clásico de *missed wakeups* de las variables de
  condición.

### Apagado ordenado (graceful shutdown)

Tanto cliente como servidor manejan `SIGTERM` para terminar de forma prolija
en lugar de dejar conexiones a medio abrir:

- **Servidor**: el handler de `SIGTERM` (que siempre corre en el thread
  principal) marca un `threading.Event` (`_shutdown_requested`), cierra el
  socket de escucha —para desbloquear el `accept()` que, por PEP 475, Python
  reintenta automáticamente tras una señal— y fuerza el cierre de todos los
  sockets de clientes activos (registrados en `_client_sockets`) para
  desbloquear cualquier thread detenido en `recv`. También notifica la
  condition variable del quorum, para que ninguna agencia quede esperando
  indefinidamente a otras que quizás nunca terminen.

- **Cliente**: usa `signal.NotifyContext` para obtener un `context.Context`
  que se cancela automáticamente al recibir `SIGTERM`. Un goroutine separado
  espera a que el contexto se cancele y cierra la conexión, lo que desbloquea
  cualquier llamada bloqueante de red en curso (`Read`/`Write`). El resto del
  código distingue si un error de red ocurrió por una desconexión real o
  porque el propio proceso decidió cerrar la conexión (`isShutdown`), para no
  reportar como fallo un cierre solicitado.

En ambos casos, el objetivo es el mismo: convertir una señal asíncrona del
sistema operativo en una condición que el código puede observar y reaccionar
de forma cooperativa, sin dejar threads/goroutines bloqueados indefinidamente
en I/O.

## Manejo de memoria en el cliente

Dado que el cliente procesa archivos de entrada de tamaño variable (desde
unos pocos miles hasta cientos de miles de apuestas), se optimizó para que su
consumo de memoria no crezca proporcionalmente al tamaño del archivo:

- Los buffers de codificación de cada batch (`payloadBuffer`) y el slice de
  apuestas acumuladas (`batch`) se **reutilizan** entre batches en lugar de
  reservarse de nuevo en cada iteración.
- El archivo de entrada se procesa en dos pasadas usando `Seek` en lugar de
  mantener todas las apuestas en memoria: la primera pasada envía las
  apuestas; la segunda, tras recibir la lista de ganadores, vuelve a leer el
  archivo línea por línea para extraer únicamente el documento de cada línea
  (sin volver a parsear la apuesta completa) y así decidir si esa línea va al
  archivo de salida.
- Se ajustó el porcentaje de crecimiento del recolector de basura de Go
  (`debug.SetGCPercent`) y se fuerza periódicamente la devolución de memoria
  liberada al sistema operativo (`debug.FreeOSMemory`), ya que el
  recolector, por defecto, devuelve memoria al SO de forma gradual en
  segundo plano — comportamiento que no llega a manifestarse en un proceso
  de vida tan corta como este cliente.

## Limitaciones conocidas / trabajo a futuro

- El servidor relee la totalidad de `bets.csv` cada vez que una agencia
  finaliza, para calcular sus ganadores. Es correcto pero no escala
  linealmente con la cantidad de agencias; una mejora posible sería mantener
  un índice en memoria por `agency_id`.
- El lock de almacenamiento es único y global, por lo que todas las agencias
  se serializan al leer/escribir el archivo compartido. Se priorizó la
  simplicidad y la correctividad por sobre el paralelismo en el acceso a
  disco.
