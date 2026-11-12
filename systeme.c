#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
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

void arreter_serveur(void) {
    entrer();
    serveur.arret = 1;
    while (serveur.nb_cam_en_attente > 0)
        signaler(&serveur.place_libre, &serveur.nb_cam_en_attente);
    while (serveur.nb_ecran_en_attente > 0)
        signaler(&serveur.objet_dispo, &serveur.nb_ecran_en_attente);
    sortir();
}

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

void *surveiller_panne(void *arg) {
    (void)arg;
    while (!breakdown_flag)
        usleep(1000);
    arreter_serveur();
    return NULL;
}

int vers_secondaire[2];
int vers_principal[2];

int lire_ligne(int fd, char *ligne, int taille) {
    int n = 0;
    while (n < taille - 1) {
        char c;
        ssize_t recu = read(fd, &c, 1);
        if (recu <= 0)
            return 0;
        if (c == '\n')
            break;
        ligne[n++] = c;
    }
    ligne[n] = '\0';
    return 1;
}

void serveur_secondaire(void) {
    close(vers_secondaire[1]);
    close(vers_principal[0]);

    printf("[SECONDAIRE pid=%d] En attente de la copie.\n", getpid());
    fflush(stdout);

    char copie[BUFFER_SIZE][64];
    int nb = 0;
    char ligne[64];
    while (nb < BUFFER_SIZE && lire_ligne(vers_secondaire[0], ligne, 64)) {
        if (strcmp(ligne, "FIN") == 0)
            break;
        strcpy(copie[nb], ligne);
        nb++;
    }

    printf("[SECONDAIRE pid=%d] Copie de %d video(s) :\n", getpid(), nb);
    int i;
    for (i = 0; i < nb; i++)
        printf("  %s\n", copie[i]);
    fflush(stdout);

    write(vers_principal[1], "CONFIRMATION\n", 13);

    /* Attend que le principal demande le retour des vidéos */
    if (!lire_ligne(vers_secondaire[0], ligne, 64))
        return;
    for (i = 0; i < nb; i++) {
        char envoi[64];
        snprintf(envoi, sizeof(envoi), "%s\n", copie[i]);
        write(vers_principal[1], envoi, strlen(envoi));
    }
    write(vers_principal[1], "FIN\n", 4);

    close(vers_secondaire[0]);
    close(vers_principal[1]);
}

void envoyer_copie(pid_t pid_secondaire) {
    printf("[PRINCIPAL pid=%d] Envoi du tampon au secondaire (pid=%d).\n",
           getpid(), pid_secondaire);

    int i;
    for (i = 0; i < serveur.nb_videos; i++) {
        int indice = (serveur.indice_retrait + i) % BUFFER_SIZE;
        char ligne[64];
        snprintf(ligne, sizeof(ligne), "%s\n", serveur.videos[indice]);
        write(vers_secondaire[1], ligne, strlen(ligne));
    }
    write(vers_secondaire[1], "FIN\n", 4);

    char reponse[64];
    lire_ligne(vers_principal[0], reponse, 64);
    printf("[PRINCIPAL pid=%d] Confirmation recue : %s\n", getpid(), reponse);
}

/* Vide le tampon et autorise de nouveau les dépôts et les retraits. */
void reinitialiser_tampon(void) {
    entrer();
    int i;
    for (i = 0; i < BUFFER_SIZE; i++) {
        free(serveur.videos[i]);
        serveur.videos[i] = NULL;
    }
    serveur.indice_depot = 0;
    serveur.indice_retrait = 0;
    serveur.nb_videos = 0;
    serveur.arret = 0;
    sortir();
}

/* Demande au secondaire les vidéos sauvées et les remet dans le tampon. */
void recevoir_copie(void) {
    write(vers_secondaire[1], "RETOUR\n", 7);

    char ligne[64];
    while (lire_ligne(vers_principal[0], ligne, 64)) {
        if (strcmp(ligne, "FIN") == 0)
            break;
        char *video = malloc(64);
        if (!video) {
            perror("malloc");
            exit(1);
        }
        strcpy(video, ligne);
        if (!deposer(video))
            free(video);
    }

    close(vers_secondaire[1]);
    close(vers_principal[0]);
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    srand(time(NULL));

    if (pipe(vers_secondaire) < 0 || pipe(vers_principal) < 0) {
        perror("pipe");
        exit(1);
    }

    pid_t pid_secondaire = fork();
    if (pid_secondaire < 0) {
        perror("fork");
        exit(1);
    }
    if (pid_secondaire == 0) {
        serveur_secondaire();
        exit(0);
    }
    close(vers_secondaire[0]);
    close(vers_principal[1]);

    struct sigaction sa;
    sa.sa_handler = handle_breakdown;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGALRM, &sa, NULL);

    // Simulation d'une panne du serveur principal après un délai aléatoire.
    alarm(rand() % 11 + 5);

    pthread_t cameras[N_CAMERAS];
    pthread_t ecrans[N_SCREENS];
    pthread_t panne;
    int i;

    for (i = 0; i < N_CAMERAS; i++)
        pthread_create(&cameras[i], NULL, boucle_camera, NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_create(&ecrans[i], NULL, boucle_ecran, NULL);
    pthread_create(&panne, NULL, surveiller_panne, NULL);

    for (i = 0; i < N_CAMERAS; i++)
        pthread_join(cameras[i], NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_join(ecrans[i], NULL);
    pthread_join(panne, NULL);

    printf("Tampon figé (%d vidéos) :\n", serveur.nb_videos);
    for (i = 0; i < serveur.nb_videos; i++) {
        int indice = (serveur.indice_retrait + i) % BUFFER_SIZE;
        printf("  %s\n", serveur.videos[indice]);
    }

    envoyer_copie(pid_secondaire);

    printf("Appuyez sur Entree pour redemarrer le serveur principal.\n");
    fflush(stdout);
    char touche[16];
    fgets(touche, sizeof(touche), stdin);

    reinitialiser_tampon();
    recevoir_copie();
    waitpid(pid_secondaire, NULL, 0);

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
