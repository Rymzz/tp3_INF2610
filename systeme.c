#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <time.h>
#include <errno.h>

// -------- configuration --------
#define BUFFER_SIZE 5
#define N_CAMERAS 3
#define N_SCREENS 2
// Rappel de l'énoncé : le total caméras + écrans doit être > 4.

// ------------------- Simulation de caméra/écran (fournies, ne pas modifier) -------------------
static int video_id = 0;
char *createVideo() {
    char *buf = malloc(64);
    if (!buf) { perror("malloc"); exit(1); }
    sprintf(buf, "Video-%d", video_id++);
    return buf;
}
void broadcastVideo(const char *video) {
    printf("[SCREEN] Diffusion: %s\n", video);
}

// ------------------- Signal : simulation de panne (fourni) -------------------
// alarm() est déclenché avec un délai aléatoire dans main() : quand il expire,
// ce handler est appelé. Ne fais PAS de travail lourd dans un handler de
// signal (voir le cours) : contente-toi de lever un drapeau.
volatile sig_atomic_t breakdown_flag = 0;

void handle_breakdown(int sig) {
    (void)sig;
    breakdown_flag = 1;
}

/* TODO (Section 1) : Mise en place du système de vidéosurveillance
 * ------------------------------------------------------------------
 *
 * TODO (Section 2) : Intégrité des données lors d'une panne
 * ------------------------------------------------------------------
 */

/* File circulaire des vidéos du serveur central. 
(On libère une vidéo seulement après broadcastVideo)*/
typedef struct {
    char *videos[BUFFER_SIZE];
    int indice_depot;
    int indice_retrait;
    int nb_videos;

    
    pthread_mutex_t mutex;
    pthread_cond_t place_libre;
    pthread_cond_t objet_dispo;
    pthread_cond_t signaleurs;
    int nb_cam_en_attente;
    int nb_ecran_en_attente;
    int nb_signaleurs;
    int arret;
} Serveur;

Serveur serveur = {
    .videos = { NULL, NULL, NULL, NULL, NULL },
    .indice_depot = 0,
    .indice_retrait = 0,
    .nb_videos = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .place_libre = PTHREAD_COND_INITIALIZER,
    .objet_dispo = PTHREAD_COND_INITIALIZER,
    .signaleurs = PTHREAD_COND_INITIALIZER,
    .nb_cam_en_attente = 0,
    .nb_ecran_en_attente = 0,
    .nb_signaleurs = 0,
    .arret = 0
};

/* Moniteur de Hoare : celui qui signale cède sa place tout de suite
 * au thread qu'il vient de réveiller. */
void entrer(void) {
    pthread_mutex_lock(&serveur.mutex);
}

void attendre(pthread_cond_t *condition, int *nb_en_attente) {
    (*nb_en_attente)++;
    if (serveur.nb_signaleurs > 0)
        pthread_cond_signal(&serveur.signaleurs);
    pthread_cond_wait(condition, &serveur.mutex);
    (*nb_en_attente)--;
}

void signaler(pthread_cond_t *condition, int *nb_en_attente) {
    if (*nb_en_attente > 0) {
        serveur.nb_signaleurs++;
        pthread_cond_signal(condition);
        pthread_cond_wait(&serveur.signaleurs, &serveur.mutex);
        serveur.nb_signaleurs--;
    }
}

void sortir(void) {
    if (serveur.nb_signaleurs > 0)
        pthread_cond_signal(&serveur.signaleurs);
    pthread_mutex_unlock(&serveur.mutex);
}

/* Retourne 0 si arret est vrai : la vidéo n'a pas été déposée. */
int deposer(char *video) {
    entrer();
    while (!serveur.arret && serveur.nb_videos == BUFFER_SIZE)
        attendre(&serveur.place_libre, &serveur.nb_cam_en_attente);
    if (serveur.arret) {
        sortir();
        return 0;
    }
    serveur.videos[serveur.indice_depot] = video;
    serveur.indice_depot = (serveur.indice_depot + 1) % BUFFER_SIZE;
    serveur.nb_videos++;
    signaler(&serveur.objet_dispo, &serveur.nb_ecran_en_attente);
    sortir();
    return 1;
}

/* Retourne NULL si arret est vrai. */
char *retirer(void) {
    entrer();
    while (!serveur.arret && serveur.nb_videos == 0)
        attendre(&serveur.objet_dispo, &serveur.nb_ecran_en_attente);
    if (serveur.arret) {
        sortir();
        return NULL;
    }
    char *video = serveur.videos[serveur.indice_retrait];
    serveur.videos[serveur.indice_retrait] = NULL;
    serveur.indice_retrait = (serveur.indice_retrait + 1) % BUFFER_SIZE;
    serveur.nb_videos--;
    signaler(&serveur.place_libre, &serveur.nb_cam_en_attente);
    sortir();
    return video;
}

/* Met arret à 1 et réveille les threads endormis pour qu'ils le voient. */
void arreter_serveur(void) {
    entrer();
    serveur.arret = 1;
    while (serveur.nb_cam_en_attente > 0)
        signaler(&serveur.place_libre, &serveur.nb_cam_en_attente);
    while (serveur.nb_ecran_en_attente > 0)
        signaler(&serveur.objet_dispo, &serveur.nb_ecran_en_attente);
    sortir();
}

/* createVideo modifie video_id : une seule caméra à la fois. */
static pthread_mutex_t mutex_creation = PTHREAD_MUTEX_INITIALIZER;

void *boucle_camera(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&mutex_creation);
        char *video = createVideo();
        pthread_mutex_unlock(&mutex_creation);
        if (!deposer(video)) {
            free(video);
            break;
        }
    }
    return NULL;
}

void *boucle_ecran(void *arg) {
    (void)arg;
    for (;;) {
        char *video = retirer();
        if (video == NULL)
            break;
        broadcastVideo(video);
        free(video);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    srand(time(NULL));

    struct sigaction sa;
    sa.sa_handler = handle_breakdown;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGALRM, &sa, NULL);

    // Simulation d'une panne du serveur principal après un délai aléatoire.
    alarm(rand() % 11 + 5);

    pthread_t cameras[N_CAMERAS];
    pthread_t ecrans[N_SCREENS];
    int i;

    for (i = 0; i < N_CAMERAS; i++)
        pthread_create(&cameras[i], NULL, boucle_camera, NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_create(&ecrans[i], NULL, boucle_ecran, NULL);

    for (i = 0; i < N_CAMERAS; i++)
        pthread_join(cameras[i], NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_join(ecrans[i], NULL);

    return 0;
}
