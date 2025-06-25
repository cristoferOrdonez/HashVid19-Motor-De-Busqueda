#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h> 
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <poll.h>
#include <sys/shm.h>
#include <signal.h>

// Definiciones de tamaño para la tabla hash y rutas de archivos
#define TAMANO_TABLA 1000003 
//#define ARCHIVO_DATASET "/media/farid/OS1/Users/farid/Downloads/archive/full_dataset_clean.csv" 
#define ARCHIVO_DATASET "full_dataset_clean.csv" 
//#define ARCHIVO_INDICES "/media/farid/OS1/Users/farid/Downloads/archive/index.bin"
#define ARCHIVO_INDICES "index.bin"

// llaves para memoira compartida
#define LLAVE_SHM_FECHA 123
#define LLAVE_SHM_TIEMPO_INICIAL 456
#define LLAVE_SHM_TIEMPO_FINAL 986
#define LLAVE_SHM_IDIOMA 789
#define LLAVE_SHM_RESULTADOS 147

#define LIMITE_BUFFER_RESULTADOS 40000

// rutas para la creación de tuberias
#define RUTA_FIFO_INDICE_TERMINADO "fifo_indice_terminado"
#define RUTA_FIFO_RESULTADOS "fifo_resultados"
#define RUTA_FIFO_CONTINUIDAD "fifo_continuidad"

// posibles estados de continuidad para el proceso de consulta y apertura
#define ESTADO_CONTINUAR "c"
#define ESTADO_SALIR "s"

// descriptores de tuberias
int fd_indice_creado, fd_resultados, fd_continuidad;

// ids para la creación de memoria compartida
int shm_id_fecha, shm_id_idioma, shm_id_hora_inicial, shm_id_hora_final;

// apuntadores para el manejo de memoria compartida
char *ap_fecha_shm, *ap_idioma_shm, *ap_tiempo_inicial_shm, *ap_tiempo_final_shm;

// parámetros de búsqueda
char fecha[11], tiempo_inicial[9], tiempo_final[9], idioma[4];

// estructura para la información de los tweets
struct Tweet {
    long id;
    char fecha[11];   // YYYY-MM-DD
    char tiempo[9];   // HH:MM:SS
    char idioma[4];   // ej: "es", "en"
    char pais[4];     // ej: "DE", "PA"
};

// contiene un puntero apuntando a un registro en el CSV y un puntero al siguiente nodo de colisión.
struct NodoIndice { 
    int64_t offset_csv;       
    int64_t offset_siguiente_nodo; 
};

void leer_en_tuberia(int fd, void *receptor, int tamaño){

    int r = read(fd, receptor, tamaño);

    // verificación del error
    if (r != tamaño){
        if(r==-1){
            perror("Error al momento de leer");
        }else{
            printf("Error al momento de leer: no se ha podido leer todos los bytes.");
        }
        exit(-1);
    
    }

}

void escribir_en_tuberia(int fd, void *contenido, int tamaño){

    int r = write(fd, contenido, tamaño);

    // verificación del error
    if (r != tamaño){
        if(r==-1){
            perror("Error al momento de escribir:");
        }
        else{
            perror("Error al momento de escribir, no se escribieron todos los bytes:");
        }
        exit(-1);
    }

}

// función Hash (utilizando el algoritmo djb2)
unsigned long hash_djb2(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }
    return hash;
}

// función para crear la tabla hash en disco (indexando el apuntador al registro en el CSV)
void crear_tabla_hash() {
    FILE *dataset_fd, *indice_fd;
    int64_t *tabla_indices; // Esta es la tabla de "cabeceras" de las listas enlazadas.

    printf("Asignando memoria para la tabla de cabeceras del índice (%.2f MB)...\n", (double)TAMANO_TABLA * sizeof(int64_t) / (1024*1024));
    tabla_indices = (int64_t *)malloc(TAMANO_TABLA * sizeof(int64_t));
    if (!tabla_indices) {
        perror("Error al asignar memoria para la tabla de índice");
        exit(EXIT_FAILURE);
    }
    
    /// inicialización todas las cabeceras a -1, indicando listas vacías.
    for (size_t i = 0; i < TAMANO_TABLA; i++) {
        tabla_indices[i] = -1;
    }

    dataset_fd = fopen(ARCHIVO_DATASET, "r");
    if (!dataset_fd) {
        perror("Error al abrir el dataset de entrada");
        free(tabla_indices);
        exit(EXIT_FAILURE);
    }
    
    // Apertura del archivo de índice en modo escritura binaria.
    indice_fd = fopen(ARCHIVO_INDICES, "wb");
    if (!indice_fd) {
        perror("Error al crear el archivo de índice binario");
        fclose(dataset_fd);
        free(tabla_indices);
        exit(EXIT_FAILURE);
    }
    
    // creación del indice

    // escritura del espacio reservado para la tabla de cabeceras, que se llenará al final
    printf("Escribiendo cabecera de índice en %s...\n", ARCHIVO_INDICES);
    fwrite(tabla_indices, sizeof(int64_t), TAMANO_TABLA, indice_fd);

    printf("Procesando el dataset y construyendo el índice...\n");
    char buffer_linea[256];
    long long contador_tweets = 0;
    
    // guardado del offset después de la cabecera del CSV para empezar a leer datos reales
    int64_t offset_csv_actual = ftell(dataset_fd);

    while (fgets(buffer_linea, sizeof(buffer_linea), dataset_fd)) {
        char cadena_fecha[11] = {0};
        char cadena_hora[3] = {0};

        // parseado de la fecha, necesaria para el hash
        if (sscanf(buffer_linea, "%*[^,],%10[^,]", cadena_fecha) != 1) {
            offset_csv_actual = ftell(dataset_fd); // actualización del offset para la siguiente línea
            continue; // si la línea no tiene el formato esperado, se salta
        }

        // parseado de la hora (HH) del campo tiempo (HH:MM:SS)
        char *token_tiempo_inicio = strchr(buffer_linea, ','); // se salta el id
        if (token_tiempo_inicio) {
            token_tiempo_inicio = strchr(token_tiempo_inicio + 1, ','); // se salta la fecha
        }
        if (token_tiempo_inicio) {
            if (token_tiempo_inicio) {
                strncpy(cadena_hora, token_tiempo_inicio + 1, 2); // se copian los dos primeros caracteres (HH)
                cadena_hora[2] = '\0';
            } else {
                offset_csv_actual = ftell(dataset_fd);
                continue;
            }
        } else {
            offset_csv_actual = ftell(dataset_fd);
            continue;
        }

        // se concatena la fecha y la hora para el hash
        size_t tamano_resultado = strlen(cadena_fecha) + strlen(cadena_hora) + 1;
        char cadena_concatenada[tamano_resultado];
        snprintf(cadena_concatenada, tamano_resultado, "%s%s", cadena_fecha, cadena_hora);

        // calculo del hash y el índice para la fecha y la hora
        unsigned long valor_hash = hash_djb2(cadena_concatenada);
        unsigned int indice = valor_hash % TAMANO_TABLA;

        // creación del nuevo nodo del índice
        struct NodoIndice nuevo_nodo;
        nuevo_nodo.offset_csv = offset_csv_actual;      
        nuevo_nodo.offset_siguiente_nodo = tabla_indices[indice]; // el nuevo nodo apunta a la antigua cabecera de la lista

        // escritura del nuevo nodo al final del archivo de índice
        fseek(indice_fd, 0, SEEK_END);
        int64_t new_node_offset = ftell(indice_fd); // posición en la que se escribe el nodo
        fwrite(&nuevo_nodo, sizeof(struct NodoIndice), 1, indice_fd);

        // actualizar la tabla de cabeceras en memoria para que apunte a este nuevo nodo
        tabla_indices[indice] = new_node_offset;

        contador_tweets++;
        if (contador_tweets % 1000000 == 0) {
            printf("Procesados %lld millones de registros...\n", contador_tweets / 1000000);
        }
        
        // preparación del offset para la siguiente iteración
        offset_csv_actual = ftell(dataset_fd);
    }
    printf("Procesamiento completo. Total de registros indexados: %lld\n", contador_tweets);

    // se vuelve al inicio del archivo de índice y se escribe la tabla de cabeceras al final
    printf("Escribiendo la tabla de cabeceras final en %s...\n", ARCHIVO_INDICES);
    fseek(indice_fd, 0, SEEK_SET);
    fwrite(tabla_indices, sizeof(int64_t), TAMANO_TABLA, indice_fd);
    
    printf("¡Creación del índice finalizada!\n");

    fclose(dataset_fd);
    fclose(indice_fd);
    free(tabla_indices);
}

// función para crear y anclar un segmento de memoria compartida
void *inicializar_memoria_compartida(long tamano, key_t llave, int *shmId){

    void *ap;

    // crear segmento de memoria compartida
	*shmId = shmget(llave, tamano, 0666 | IPC_CREAT);
    
	// verificacion de error
	if (*shmId < 0){
    	perror("Error error al crear el segmento de memoria compartida:");
        exit(-1);
	}

    // anclar segmento de memoria compartida
    ap = shmat(*shmId, 0, 0);

    // verificación del error
    if (ap == NULL){
    	perror("Error al crear el apuntador:");
        exit(-1);
	}

    return ap;

}

// función para desanclar un segmento de memoria compartida
void cerrar_memoria_compartida(void* ap, int shmId){

    // se desprende el segmento de memoria compartida (la interfaz de cliente eliminará el segmento de memoria compartida)
    int r = shmdt(ap);

    // verificacion del error
    if(r == -1){ 
        perror("Error al desprender la memoria compartida:");
        exit(-1);
    }
}

// función para buscar en el CSV usando la tabla hash 
void busqueda() {
    FILE *indice_fd, *dataset_fd;
    int64_t *index_table;
    
    index_table = (int64_t *)malloc(TAMANO_TABLA * sizeof(int64_t));
    if (!index_table) {
        perror("Error al asignar memoria para la tabla de índice");
        return;
    }

    indice_fd = fopen(ARCHIVO_INDICES, "rb");
    if (!indice_fd) {
        perror("No se pudo abrir el archivo de índice. ¿Ejecutaste la creación primero?");
        free(index_table);
        return;
    }
    
    // leer solo la tabla de cabeceras del archivo de índice
    fread(index_table, sizeof(int64_t), TAMANO_TABLA, indice_fd);

    dataset_fd = fopen(ARCHIVO_DATASET, "r");
    if (!dataset_fd) {
        perror("No se pudo abrir el archivo de datos CSV");
        fclose(indice_fd);
        free(index_table);
        return;
    }

    char hora_busqueda[3];
    strncpy(hora_busqueda, tiempo_inicial, 2);
    hora_busqueda[2] = '\0';

    // se concatena la fecha y la hora para el hash
    size_t tamano_resultado_busqueda = strlen(fecha) + strlen(hora_busqueda) + 1;
    char cadena_concatenada_busqueda[tamano_resultado_busqueda];
    snprintf(cadena_concatenada_busqueda, tamano_resultado_busqueda, "%s%s", fecha, hora_busqueda);

    // calculo del índice hash para la fecha y la hora de búsqueda.
    unsigned long valor_hash = hash_djb2(cadena_concatenada_busqueda);
    unsigned int indice = valor_hash % TAMANO_TABLA;
    int64_t offset_nodo_actual = index_table[indice];

    // si la cabecera es -1, no hay entradas para este hash
    if (offset_nodo_actual == -1) {
        int concidencias = 0;

        escribir_en_tuberia(fd_resultados, &concidencias, sizeof(concidencias));

    } else {
        int cantidad_coincidencias = 0, shm_id_resultados;
        struct NodoIndice nodo_actual;
        char buffer_linea[256];

        struct Tweet buffer[LIMITE_BUFFER_RESULTADOS];

        struct Tweet *ap_tweets;
        
        // recorrido de la lista enlazada de nodos dentro del archivo de índice para manejar colisiones
        while (offset_nodo_actual != -1 && cantidad_coincidencias < LIMITE_BUFFER_RESULTADOS) {
            // se salta a la posición del nodo actual en el archivo de índice
            fseek(indice_fd, offset_nodo_actual, SEEK_SET);
            fread(&nodo_actual, sizeof(struct NodoIndice), 1, indice_fd);

            // ee salta a la posición del registro en el archivo CSV
            fseek(dataset_fd, nodo_actual.offset_csv, SEEK_SET);
            fgets(buffer_linea, sizeof(buffer_linea), dataset_fd);
            
            // se parsea la línea del CSV para verificar la fecha (evitar colisiones)
            struct Tweet tweet = {0};
            char *token_dataset;
            char *puntero_guardado;
            buffer_linea[strcspn(buffer_linea, "\n")] = 0; // se elimina salto de línea

            token_dataset = strtok_r(buffer_linea, ",", &puntero_guardado);
            if (token_dataset) tweet.id = atoll(token_dataset);
            token_dataset = strtok_r(NULL, ",", &puntero_guardado);
            if (token_dataset) strncpy(tweet.fecha, token_dataset, sizeof(tweet.fecha) - 1);
            token_dataset = strtok_r(NULL, ",", &puntero_guardado);
            if (token_dataset) strncpy(tweet.tiempo, token_dataset, sizeof(tweet.tiempo) - 1);
            token_dataset = strtok_r(NULL, ",", &puntero_guardado);
            if (token_dataset) strncpy(tweet.idioma, token_dataset, sizeof(tweet.idioma) - 1);
            token_dataset = strtok_r(NULL, "\n\r", &puntero_guardado);
            if (token_dataset) strncpy(tweet.pais, token_dataset, sizeof(tweet.pais) - 1);

             // se verifica si el registro cumple con todos los criterios de búsqueda.
            if (strcmp(tweet.fecha, fecha) == 0 &&
                strcmp(tweet.idioma, idioma) == 0 &&
                strcmp(tweet.tiempo, tiempo_inicial) >= 0 &&  // debe ser mayor o igual al tiempo inicial
                strcmp(tweet.tiempo, tiempo_final) <= 0)      // debe ser menor o igual al tiempo final
            {    
                buffer[cantidad_coincidencias] = tweet;
                cantidad_coincidencias++;
            }
            
            // se mueve al siguiente nodo en la lista enlazada (cadena de colisión)
            offset_nodo_actual = nodo_actual.offset_siguiente_nodo;
        }

        if(cantidad_coincidencias == 0){

            escribir_en_tuberia(fd_resultados, &cantidad_coincidencias, sizeof(cantidad_coincidencias));

        } else {

            // inicialización de memoria compartida para envio de resultados al proceso p1-dataProgram
            ap_tweets = (struct Tweet*) inicializar_memoria_compartida(sizeof(struct Tweet)*cantidad_coincidencias, LLAVE_SHM_RESULTADOS, &shm_id_resultados);

            memcpy(ap_tweets, buffer, sizeof(struct Tweet)*cantidad_coincidencias);

            cerrar_memoria_compartida(ap_tweets, shm_id_resultados);

            // enviar la cantidad de resultados encontrados a través de la tubería
            escribir_en_tuberia(fd_resultados, &cantidad_coincidencias, sizeof(cantidad_coincidencias));

        }

    }

    // cierre de desciptores
    fclose(dataset_fd);
    fclose(indice_fd);
    free(index_table);
}

int existe_archivo(const char *path) {
    return access(path, F_OK) == 0;
}

int abrir_tuberia_escritura(const char *fifo_path){

    int fd = open(fifo_path, O_WRONLY); // siempre se abren las tuberias en modo bloqueante

    // verificación del error
    if(fd == -1){
        perror("Error al abrir la tuberia"); 
        exit(-1);
    }

    return fd;

}

int abrir_tuberia_lectura(const char *fifo_path){

    int fd = open(fifo_path, O_RDONLY); // siempre se abren las tuberias en modo bloqueante

    // verificación del error
    if(fd == -1){
        perror("Error al abrir la tuberia");
        exit(-1);
    }

    return fd;

}

void cerrar_proceso(){

    // cierre de memoria compartida y descriptores
    cerrar_memoria_compartida(ap_fecha_shm, shm_id_fecha);
    cerrar_memoria_compartida(ap_tiempo_inicial_shm, shm_id_hora_inicial);
    cerrar_memoria_compartida(ap_tiempo_final_shm, shm_id_hora_final);
    cerrar_memoria_compartida(ap_idioma_shm, shm_id_idioma);
    close(fd_indice_creado);
    close(fd_resultados);
    close(fd_continuidad);

    kill(getpid(),SIGTERM); // SIGTERM se considera una forma menos abrupta para cerrar un programa

}


int main() {

    // se bloquea la señal de salida SIGINT (ocurre cuando se oprime ctrl+C)
    signal(SIGINT, NULL);

    sleep(1);

    char testigo[2] = "x";

    fd_continuidad = abrir_tuberia_lectura(RUTA_FIFO_CONTINUIDAD);

    // el proceso p1-dataProgram le indica al proceso p2-dataProgram que debe continuar con la ejecución
    leer_en_tuberia(fd_continuidad, testigo, sizeof(testigo));

    fd_indice_creado = abrir_tuberia_escritura(RUTA_FIFO_INDICE_TERMINADO);
    fd_resultados = abrir_tuberia_escritura(RUTA_FIFO_RESULTADOS);

    ap_fecha_shm = (char*) inicializar_memoria_compartida(sizeof(char[11]), LLAVE_SHM_FECHA, &shm_id_fecha);
    ap_tiempo_inicial_shm = (char*) inicializar_memoria_compartida(sizeof(char[9]), LLAVE_SHM_TIEMPO_INICIAL, &shm_id_hora_inicial);
    ap_tiempo_final_shm = (char*) inicializar_memoria_compartida(sizeof(char[9]), LLAVE_SHM_TIEMPO_FINAL, &shm_id_hora_final);
    ap_idioma_shm = (char*) inicializar_memoria_compartida(sizeof(char[4]), LLAVE_SHM_IDIOMA, &shm_id_idioma);
    
    // crea el indice si aun no ha sido creado
    if(!existe_archivo(ARCHIVO_INDICES)){
        crear_tabla_hash();
    }
    
    // le indica al proceso p1-dataProgram que el indice ya ha sido creado
    escribir_en_tuberia(fd_indice_creado, testigo, sizeof(testigo));

    while(true) {
    
        // el proceso verifica si debe continuar o dejar de ejecutarse
        leer_en_tuberia(fd_continuidad, testigo, sizeof(testigo));

        if(strcmp(testigo, ESTADO_SALIR) == 0){

            cerrar_proceso(); // el programa se cierra adecuadamente

        }
        
        // se obtienen los criterios de bsuqueda a través de memoria compartida
        strcpy(fecha, ap_fecha_shm);
        strcpy(tiempo_inicial, ap_tiempo_inicial_shm);
        strcpy(tiempo_final, ap_tiempo_final_shm);
        strcpy(idioma, ap_idioma_shm);

        // se realiza la bsuqueda
        busqueda();
    
    }

    return 0;
}