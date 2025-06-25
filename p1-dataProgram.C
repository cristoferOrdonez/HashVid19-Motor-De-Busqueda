#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <signal.h>
#include <time.h>

// opciones del menu
#define INGRESAR_PRIMER_CRITERIO 1
#define INGRESAR_SEGUNDO_CRITERIO 2
#define INGRESAR_TERCER_CRITERIO 3
#define BUSCAR 4
#define VER_CRITERIOS 5
#define SALIR 6

// llaves para memoira compartida
#define LLAVE_SHM_FECHA 123
#define LLAVE_SHM_TIEMPO_INICIAL 456
#define LLAVE_SHM_TIEMPO_FINAL 986
#define LLAVE_SHM_IDIOMA 789
#define LLAVE_SHM_RESULTADOS 147

// medidas de ancho para la impresion de resultados
#define ANCHO_ID 20
#define ANCHO_FECHA 12
#define ANCHO_TIEMPO 10
#define ANCHO_IDIOMA 8
#define ANCHO_PAIS 8

// rutas para la creación de tuberias
#define RUTA_FIFO_INDICE_TERMINADO "fifo_indice_terminado"
#define RUTA_FIFO_RESULTADOS "fifo_resultados"
#define RUTA_FIFO_CONTINUIDAD "fifo_continuidad"

// posibles estados de continuidad para el proceso de consulta y apertura
#define ESTADO_CONTINUAR "c"
#define ESTADO_SALIR "s"

// cirterios ingresados por el usuario para la busqueda de tweets
char criterio_fecha[11], criterio_idioma[4], criterio_tiempo_inicio[9], criterio_tiempo_final[9];

// descriptores de tuberias
int fd_indice_creado, fd_resultados, fd_continuidad;

// ids para la creación de memoria compartida
int id_shm_fecha, id_shm_idioma, id_shm_tiempo_inicial, id_shm_tiempo_final, id_shm_resultados;

// apuntadores para el manejo de memoria compartida
char *ap_fecha, *ap_idioma, *ap_tiempo_inicial, *ap_tiempo_final;

// estructura para la información de los tweets
struct Tweet{
    long id;
    char fecha[11];  // YYYY-MM-DD
    char tiempo[9];   // HH:MM:SS
    char idioma[4];   // ej: "es", "en", "und"
    char pais[4];   // ej: "DE", "RU"
};

void mostrar_menu(){

    printf("\n1. Ingresar primer criterio de busqueda (fecha).\n");
    printf("2. Ingresar segundo criterio de busqueda (rango de tiempo).\n");
    printf("3. Ingresar tercer criterio de busqueda (idioma).\n");
    printf("4. Realizar busqueda.\n");
    printf("5. Ver criterios.\n");
    printf("6. Salir.\n");

}

void leer_en_tuberia(int fd, void *receptor, int tamaño){

    int r = read(fd, receptor, tamaño);

    // verificación del error
    if (r != tamaño){
        if(r==-1){
            perror("Error al momento de leer");
        }else{
            printf("Error al momento de leer, no se ha podido leer todos los bytes");
        }
        exit(-1);
    }

}

void escribir_en_tuberia(int fd, void *contenido, int tamaño){

    int r = write(fd, contenido, tamaño);

    // verificación del error
    if (r != tamaño){
        if(r==-1){
            perror("Error al momento de escribir");
        }
        else{
            perror("Error al momento de escribir, no se escribieron todos los bytes");
        }
        exit(-1);
    }

}

// función para crear y anclar un segmento de memoria compartida
void *inicializar_memoria_compartida(long tamaño, key_t llave, int *id_shm){

    void *ap;

    // crear segmento de memoria compartida
	*id_shm = shmget(llave, tamaño, 0666 | IPC_CREAT);
    
	//verificacion de error
	if (*id_shm < 0){
    	perror("Error error al crear el segmento de memoria compartida");
        exit(-1);
	}

    // anclar segmento de memoria compartida
    ap = shmat(*id_shm, 0, 0);

    // verificación del error
    if (ap == NULL){
    	perror("Error al crear el apuntador");
        exit(-1);
	}

    return ap;

}

// función para desanclar y eliminar un segmento de memoria compartida
void cerrar_memoria_compartida(void* ap, int id_shm){

    // se desprende el segmento de memoria compartida
    int r = shmdt(ap);

    // verificacion del error
    if(r == -1){
        perror("Error al desprender la memoria compartida:");
        exit(-1);
    }

    // eliminación del segmento de memoria compartida
    r = shmctl(id_shm, IPC_RMID, NULL);

    // verificacion del error
    if(r == -1){
        perror("Error al eliminar el segmento de memoria compartida:");
        exit(-1);
    }

}

bool es_bisiesto(int año) {
    return (año % 4 == 0 && año % 100 != 0) || (año % 400 == 0);
}

// función para saber los dias de un mes, segun el mes y el año
int dias_del_mes(int mes, int año) {
    if (mes == 2) {
        return es_bisiesto(año) ? 29 : 28;
    }
    if (mes == 4 || mes == 6 || mes == 9 || mes == 11) {
        return 30;
    }
    return 31;
}

// función para limpiar posibles caracteres que se hallan quedado en el buffer
void limpiar_buffer() {
    int caracter;
    while ((caracter = getchar()) != '\n' && caracter != EOF);
}

// función para ingresar la fecha
void ingresar_primer_criterio() {

    limpiar_buffer();

    int dia, mes, año;
    char linea[100]; // buffer para leer la línea completa
    char caracter_extra; // variable para verificar si hay caracteres extra después del número
    int resultado_del_escaneo;

    // obtención del año actual
    time_t ahora = time(NULL);
    struct tm *info_tiempo = localtime(&ahora);
    int año_actual = info_tiempo->tm_year + 1900;

    // ingresar año
    do {
        printf("Por favor, ingrese el año (entre 2019 y %d):\n", año_actual);
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &año, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero. (Ej: 2019, no 2019k)\n");
                año = -1;
            }


        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            año = -1;
        }

        if (año != -1 && (año < 2019 || año > año_actual)) {
            printf("El año %d no es válido. Debe estar entre 2019 y %d.\n", año, año_actual);
            año = -1;
        }
    } while (año == -1);

    // ingresar mes
    do {
        printf("Por favor, ingrese el mes (entre 1 y 12):\n");
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &mes, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero.\n");
                mes = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            mes = -1;
        }

        if (mes != -1 && (mes < 1 || mes > 12)) {
            printf("El mes %d no es válido. Debe estar entre 1 y 12.\n", mes);
            mes = -1;
        }
    } while (mes == -1);

    // Ingresar día
    int dias_mes = dias_del_mes(mes, año);
    do {
        printf("Por favor, ingrese el día (entre 1 y %d):\n", dias_mes);
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &dia, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero.\n");
                dia = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            dia = -1;
        }

        if (dia != -1 && (dia < 1 || dia > dias_mes)) {
            printf("El día %d no es válido. Debe estar entre 1 y %d.\n", dia, dias_mes);
            dia = -1;
        }
    } while (dia == -1);

    sprintf(criterio_fecha, "%04d-%02d-%02d", año, mes, dia);
}

void ingresar_segundo_criterio(){

    limpiar_buffer();

    int hora_inicial;
    char linea[100]; // buffer para leer la línea completa
    char caracter_extra; // variable para verificar si hay caracteres extra después del número
    int resultado_del_escaneo;

    // ingresar hora inicial
    do {
        printf("Por favor, ingrese la hora inicial (entre 0 y 23)\n");
        if (fgets(linea, sizeof(linea), stdin) == NULL) {
            printf("Error al leer la entrada.\n");
            exit(1);
        }

        resultado_del_escaneo = sscanf(linea, "%d %c", &hora_inicial, &caracter_extra);

        if (resultado_del_escaneo == 2) {
            if (caracter_extra != '\n') {
                printf("Entrada no válida. Debe ingresar solo un número entero. (Ej: 12, no 12k)\n");
                hora_inicial = -1;
            }
        } else if (resultado_del_escaneo == 1) {
            // caso ideal
        } else {
            printf("Entrada no válida. Debe ingresar un número entero.\n");
            hora_inicial = -1;
        }

        if (hora_inicial != -1 && (hora_inicial < 0 || hora_inicial > 23)) {
            printf("La hora inicial %d no es valida. Debe estar entre 0 y 23.\n", hora_inicial);
            hora_inicial = -1;
        }
    } while (hora_inicial == -1);

    sprintf(criterio_tiempo_inicio, "%02d:00:00", hora_inicial);

    sprintf(criterio_tiempo_final, "%02d:59:59", hora_inicial);

    printf("\nTiempo inicial establecido en: %s\n", criterio_tiempo_inicio);
    printf("Tiempo final establecido en: %s\n", criterio_tiempo_final);
    
}

void ingresar_tercer_criterio() {

    char idioma_temporal[4] = {0}; //variable para almacenar los caracteres que ingrese el usuario
    char caracter;
        
    limpiar_buffer();
    
    while (true) {
        printf("Por favor, ingrese el código de idioma (2-3 letras): ");
                
        // se leen exactamente 3 caracteres del buffer
        int cantidad_de_caracteres = 0;
        while (true) {
            caracter = getchar();
            if (caracter == '\n' || caracter == EOF) {
                if(cantidad_de_caracteres < 4)
                    idioma_temporal[cantidad_de_caracteres] = '\0';
                break;
            }
            
            if(cantidad_de_caracteres < 4){
                idioma_temporal[cantidad_de_caracteres] = caracter;
            }

            cantidad_de_caracteres++;
        }
        
        // se valida la cantidad de caracteres ingresados
        if (cantidad_de_caracteres < 2 || cantidad_de_caracteres > 3) {
            printf("Debe ingresar exactamente 2 o 3 letras.\n");
            continue;
        }
        
        // se valida que solo se hayan ingresado caracteres alfabeticos
        bool es_valido = true;
        for (int indice_caracter_actual = 0; indice_caracter_actual < cantidad_de_caracteres; indice_caracter_actual++) {
            if (!isalpha((unsigned char)idioma_temporal[indice_caracter_actual])) {
                es_valido = false;
                break;
            }
        }
        
        if (!es_valido) {
            printf("Solo se permiten letras (a-z, A-Z).\n");
            continue;
        }
        
        // se convierte todos los caracteres a minuscula
        for (int indice_caracter_actual = 0; indice_caracter_actual < cantidad_de_caracteres; indice_caracter_actual++) {
            idioma_temporal[indice_caracter_actual] = tolower((unsigned char)idioma_temporal[indice_caracter_actual]);
        }
        
        strcpy(criterio_idioma, idioma_temporal);
        break;
    }
}

// busqueda de resultados y comunicación con el procedimiento p1-dataProgram
void buscar(){

    char testigo[2] = ESTADO_CONTINUAR;
    struct Tweet *ap_tweet;
    int cantidad_resultados;
    struct timespec inicio, final;

    clock_gettime(CLOCK_MONOTONIC, &inicio);

    // se le envian al procedimiento p2-dataProgram los criterios, a través de memoria compartida
    strcpy(ap_fecha, criterio_fecha);
    strcpy(ap_tiempo_inicial, criterio_tiempo_inicio);
    strcpy(ap_tiempo_final, criterio_tiempo_final);
    strcpy(ap_idioma, criterio_idioma);

    // se le indica (a través de una tuberia) al procedimiento p2-dataProgram que los datos ya han sido colocados en la memoria compartida
    escribir_en_tuberia(fd_continuidad, testigo, sizeof(testigo));

    // se obtiene la cantidad de resultados obtenidos
    leer_en_tuberia(fd_resultados, &cantidad_resultados, sizeof(cantidad_resultados));

    if(cantidad_resultados > 0){

        // se crea un segmento de memoira compartida para obtener los datos
        ap_tweet = (struct Tweet *) inicializar_memoria_compartida(sizeof(struct Tweet)*cantidad_resultados, LLAVE_SHM_RESULTADOS, &id_shm_resultados);

        // impresión de resulados con formato tabular
        printf("\n+");
        for (int i = 0; i < ANCHO_ID + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_FECHA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_TIEMPO + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_IDIOMA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_PAIS + 2; i++) printf("-");
        printf("+\n");

        printf("| %-*s | %-*s | %-*s | %-*s | %-*s |\n",
               ANCHO_ID, "ID",
               ANCHO_FECHA, "Fecha",
               ANCHO_TIEMPO, "Tiempo",
               ANCHO_IDIOMA, "Idioma",
               ANCHO_PAIS, "Pais");

        printf("+");
        for (int i = 0; i < ANCHO_ID + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_FECHA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_TIEMPO + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_IDIOMA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_PAIS + 2; i++) printf("-");
        printf("+\n");

        for(int posicion_tweet_actual = 0; posicion_tweet_actual < cantidad_resultados; posicion_tweet_actual++){

            if(strlen(ap_tweet[posicion_tweet_actual].pais) == 0){
                strcpy(ap_tweet[posicion_tweet_actual].pais, "-");
            }

            printf("| %-*ld | %-*s | %-*s | %-*s | %-*s |\n", 
                   ANCHO_ID, ap_tweet[posicion_tweet_actual].id,
                   ANCHO_FECHA, ap_tweet[posicion_tweet_actual].fecha,
                   ANCHO_TIEMPO, ap_tweet[posicion_tweet_actual].tiempo,
                   ANCHO_IDIOMA, ap_tweet[posicion_tweet_actual].idioma,
                   ANCHO_PAIS, ap_tweet[posicion_tweet_actual].pais);
        }

        printf("+");
        for (int i = 0; i < ANCHO_ID + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_FECHA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_TIEMPO + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_IDIOMA + 2; i++) printf("-");
        printf("+");
        for (int i = 0; i < ANCHO_PAIS + 2; i++) printf("-");
        printf("+\n");

        cerrar_memoria_compartida(ap_tweet, id_shm_resultados);

        clock_gettime(CLOCK_MONOTONIC, &final);

        long segundos = final.tv_sec - inicio.tv_sec;
        long nanosegundos = final.tv_nsec - inicio.tv_nsec;
        double tiempo_transcurrido = segundos + nanosegundos / 1000000000.0;

        printf("Se han encontrado %d resultados en %.20lf segundos.\n", cantidad_resultados, tiempo_transcurrido);

    } else { // si no se encontraron resultados, se imprime NA
        printf("\nNA\n");
    }

}

void cerrar_proceso(){

    // cierre del proceso p2-dataProgram
    char testigo[2] = ESTADO_SALIR;
    escribir_en_tuberia(fd_continuidad, testigo, sizeof(testigo));

    // cierre de memoria compartida y descriptores
    cerrar_memoria_compartida(ap_fecha, id_shm_fecha);
    cerrar_memoria_compartida(ap_tiempo_inicial, id_shm_tiempo_inicial);
    cerrar_memoria_compartida(ap_tiempo_final, id_shm_tiempo_final);
    cerrar_memoria_compartida(ap_idioma, id_shm_idioma);
    close(fd_indice_creado);
    close(fd_resultados);
    close(fd_continuidad);

}

void salir(){

    printf("Saliendo...\n");
    cerrar_proceso();
    exit(0);
}


int abrir_tuberia_para_escritura(const char *fifo_path){

    int fd = open(fifo_path, O_WRONLY); // siempre se abren las tuberias en modo bloqueante

    // verificacion del error
    if(fd == -1){
        perror("Error al abrir la tuberia");
        exit(-1);
    }

    return fd;

}

int abrir_tuberia_para_lectura(const char *fifo_path){

    int fd = open(fifo_path, O_RDONLY); // siempre se abren las tuberias en modo bloqueante

    // verificacion del error
    if(fd == -1){
        perror("Error al abrir la tuberia");
        exit(-1);
    }

    return fd;

}

void señal_de_cierre(int sig){

    printf("\nSaliendo...\n");

    cerrar_proceso();

    kill(getpid(), SIGTERM); // SIGTERM se considera una forma menos abrupta para cerrar un programa

}

void inicializar_tuberias(){

    int r;

    // eliminación de tuberías existentes para asegurar un inicio limpio
    unlink(RUTA_FIFO_INDICE_TERMINADO);
    unlink(RUTA_FIFO_RESULTADOS);
    unlink(RUTA_FIFO_CONTINUIDAD);

    // creación de las tuberías con permisos de lectura/escritura para todos

    r = mkfifo(RUTA_FIFO_INDICE_TERMINADO, 0666);
    
    // verificación del error
    if(r == -1){
        perror("Error al crear la tuberia:");
        exit(-1);
    }

    r = mkfifo(RUTA_FIFO_RESULTADOS, 0666);

    // verificación del error
    if(r == -1){
        perror("Error al crear la tuberia:");
        exit(-1);
    }

    r = mkfifo(RUTA_FIFO_CONTINUIDAD, 0666);

    // verificación del error
    if(r == -1){
        perror("Error al crear la tuberia:");
        exit(-1);
    }

}

void ver_criterios(){

    // se observa el primer caracter para saber si un criterio no ha sido inicialiado

    if(criterio_fecha[0] == '\0'){
        printf("\nNo se ha establecido una fecha.\n");
    } else {
        printf("\nFecha: %s\n", criterio_fecha);
    }

    if(criterio_tiempo_inicio[0] == '\0'){
        printf("No se ha establecido un tiempo inicial.\n");
    } else {
        printf("Tiempo inicial: %s\n", criterio_tiempo_inicio);
    }

    if(criterio_tiempo_final[0] == '\0'){
        printf("No se ha establecido un tiempo final.\n");
    } else {
        printf("Tiempo final: %s\n", criterio_tiempo_final);
    }

    if(criterio_idioma[0] == '\0'){
        printf("No se ha establecido un idioma.\n");
    } else {
        printf("Idioma: %s\n", criterio_idioma);
    }

}

int main(){

    // se capta la señal de salida SIGINT (ocurre cuando se oprime ctrl+C)
    signal(SIGINT, señal_de_cierre);

    char testigo[2] = ESTADO_CONTINUAR;
    bool informacion_faltante;

    inicializar_tuberias();

    fd_continuidad = abrir_tuberia_para_escritura(RUTA_FIFO_CONTINUIDAD);

    // se le indica al proceso p2-dataProgram que puede empezar a crear las demás tuberias
    escribir_en_tuberia(fd_continuidad, testigo, sizeof(testigo));

    fd_indice_creado = abrir_tuberia_para_lectura(RUTA_FIFO_INDICE_TERMINADO);
    fd_resultados = abrir_tuberia_para_lectura(RUTA_FIFO_RESULTADOS);

    // el proceso p2-dataProgram indica que los indices ya han sido creados
    leer_en_tuberia(fd_indice_creado, testigo, sizeof(testigo)); 

    ap_fecha = (char*) inicializar_memoria_compartida(sizeof(criterio_fecha), LLAVE_SHM_FECHA, &id_shm_fecha);
    ap_tiempo_inicial = (char*) inicializar_memoria_compartida(sizeof(criterio_tiempo_inicio), LLAVE_SHM_TIEMPO_INICIAL, &id_shm_tiempo_inicial);
    ap_tiempo_final = (char*) inicializar_memoria_compartida(sizeof(criterio_tiempo_final), LLAVE_SHM_TIEMPO_FINAL, &id_shm_tiempo_final);
    ap_idioma = (char*) inicializar_memoria_compartida(sizeof(criterio_idioma), LLAVE_SHM_IDIOMA, &id_shm_idioma);

    // inicio de la interfaz de usuario
    printf("\nBienvenido\n");

    while(true){

        mostrar_menu();

        int opcion;
        int resultado_del_escaneo = scanf("%d", &opcion);

        // verificación de contenido no numerico
        if(resultado_del_escaneo != 1){

            printf("Entrada incorrecta, por favor, ingrese un numero entero.\n");

            limpiar_buffer();

            continue;

        }


        switch (opcion)
        {
        case INGRESAR_PRIMER_CRITERIO:
            ingresar_primer_criterio();
            break;

        case INGRESAR_SEGUNDO_CRITERIO:
            ingresar_segundo_criterio();
            break;

        case INGRESAR_TERCER_CRITERIO:
            ingresar_tercer_criterio();
            break;

        case BUSCAR:

            printf("\n");

            // se verifica que todos los criterios hayan sido indicados antes de buscar
            informacion_faltante = false;

            if(criterio_fecha[0] == '\0'){
                printf("Para realizar la busqueda, debe especificar una fecha.\n");
                informacion_faltante = true;
            }

            if(criterio_tiempo_inicio[0] == '\0'){
                printf("Para realizar la busqueda, debe especificar un tiempo inicial.\n");
                informacion_faltante = true;
            }

            if(criterio_tiempo_final[0] == '\0'){
                printf("Para realizar la busqueda, debe especificar un tiempo final.\n");
                informacion_faltante = true;
            }

            if(criterio_idioma[0] == '\0'){
                printf("Para realizar la busqueda, debe especificar un idioma.\n");
                informacion_faltante = true;
            }

            if(informacion_faltante){
                continue;
            }

            buscar();

            break;  

        case VER_CRITERIOS:
            ver_criterios();
            break;

        case SALIR:
            salir();
            break; 

        default:
            printf("Comando invalido.\n");
            break;
        }

    }
    
    return 0;
}