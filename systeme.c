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
#define MAX_SAUVEGARDE (BUFFER_SIZE + N_SCREENS)
#define DUREE_DIFFUSION_MS 2000
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

    /* SECTION 2 : une vidéo retirée reste conservée ici jusqu’à validation.
     * Une case par écran : elle appartient au serveur, pas au thread. */
    char *en_cours[N_SCREENS];
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

/* Moniteur de Hoare. mutex = le verrou à obtenir (un seul thread à l'intérieur)
 * place_libre est la file d'attente des caméras
 * objet_dispo la file d'attente des écrans
 * signaleurs (File de celui qui vient de signaler)
 * Une caméra y va après avoir déposé une vidéo et signalé objet_dispo
 * écran y va après avoir retiré une vidéo et signalé place_libre */
 
void entrer(void) {
    pthread_mutex_lock(&serveur.mutex);
}

void attendre(pthread_cond_t *condition, int *nb_en_attente) {
    (*nb_en_attente)++;
    if (serveur.nb_signaleurs > 0)
        pthread_cond_signal(&serveur.signaleurs);
    pthread_cond_wait(condition, &serveur.mutex); // // Libère le mutex et attend sur la condition, puis reprend le mutex avant de continuer.
    (*nb_en_attente)--;
}

void signaler(pthread_cond_t *condition, int *nb_en_attente) {
    if (*nb_en_attente > 0) {
        serveur.nb_signaleurs++;
        pthread_cond_signal(condition);
        pthread_cond_wait(&serveur.signaleurs, &serveur.mutex); // Libère le mutex et attend sur signaleurs, puis reprend le mutex avant de continuer.
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

    // Stockes le pointeur vers la video dans la case du buffer a l'indice de depot
    serveur.videos[serveur.indice_depot] = video; 
    serveur.indice_depot = (serveur.indice_depot + 1) % BUFFER_SIZE;
    serveur.nb_videos++;
    //on signale les ecrans qui attendent dans la file objet dispo           
    signaler(&serveur.objet_dispo, &serveur.nb_ecran_en_attente);
    sortir();
    return 1;
}

/* Retourne NULL si arret est vrai */
char *retirer(int ecran) {
    entrer();
    /* Après la restauration du serveur , cet écran reprend sa vidéo interrompue */
    if (!serveur.arret && serveur.en_cours[ecran] != NULL) {
        char *video = serveur.en_cours[ecran];
        sortir();
        return video;
    }
    while (!serveur.arret && serveur.nb_videos == 0)
        attendre(&serveur.objet_dispo, &serveur.nb_ecran_en_attente);
    if (serveur.arret) {
        sortir();
        return NULL;
    }
    // Recupere le pointeur vers la video situee  à l'indice de retrait du buffer
    char *video = serveur.videos[serveur.indice_retrait];

    // Transfère la vidéo du buffer vers en_cours,
    // afin de conserver une trace de la vidéo pour la restauration après une panne
    serveur.en_cours[ecran] = video;
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

    // Reveille tous les threads en attente pour qu'ils détectent l'arrêt
    // et quittent le moniteur après la panne.
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

/* SECTION 2 : simulation d'une diffusion interruptible sans modifier

 */
int diffuser_et_valider(int ecran, char *video) {
    printf("[EN COURS écran %d] %s\n", ecran, video);
    for (int ms = 0; ms < DUREE_DIFFUSION_MS; ms += 20) {
        usleep(20000); // Attend 20 ms pour simuler le temps de diffusion de la vidéo
        entrer();
        int interrompue = serveur.arret || breakdown_flag;
        sortir();
        if (interrompue) {
            printf("[INTERROMPUE écran %d] %s conservée pour reprise\n", ecran, video);
            return 0; 
        }
    }

   
    // Revérifie l'arrêt (une panne a pu survenir depuis la derniere vérification)
    entrer();
    if (serveur.arret || breakdown_flag) {
        sortir();
        printf("[INTERROMPUE écran %d] %s conservée pour reprise\n", ecran, video);
        return 0;
    }
    broadcastVideo(video);
    serveur.en_cours[ecran] = NULL;
    free(video);
    sortir();
    return 1;
}

void *boucle_ecran(void *arg) {
    int ecran = *(int *)arg;
    for (;;) {
        char *video = retirer(ecran);
        // Si le serveur est arrêté, retirer retourne NULL, on arrête la boucle
        if (video == NULL)
            break;
        if (!diffuser_et_valider(ecran, video))
            break;
    }
    return NULL;
}

void *surveiller_panne(void *arg) {
    (void)arg;
    while (breakdown_flag == 0)
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

    char copie[MAX_SAUVEGARDE][64];
    int nb = 0;
    char ligne[64];
    while (lire_ligne(vers_secondaire[0], ligne, 64)) {
        if (strcmp(ligne, "FIN") == 0)
            break;
        if (nb < MAX_SAUVEGARDE) {
            strcpy(copie[nb], ligne);
            nb++;
        }
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
        char envoi[96];
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
        snprintf(ligne, sizeof(ligne), "Q %s\n", serveur.videos[indice]);
        write(vers_secondaire[1], ligne, strlen(ligne));
    }
    /* SECTION 2 : sauver aussi toutes les diffusions non validées.
     * Les threads ont été joints : les pointeurs ne changent plus. */
    for (int e = 0; e < N_SCREENS; e++) {
        if (serveur.en_cours[e] != NULL) {
            char ligne[96];
            snprintf(ligne, sizeof(ligne), "E %d %s\n", e, serveur.en_cours[e]);
            write(vers_secondaire[1], ligne, strlen(ligne));
        }
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
    for (int e = 0; e < N_SCREENS; e++) {
        free(serveur.en_cours[e]);
        serveur.en_cours[e] = NULL;
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
        int ecran;
        char contenu[64];
        if (sscanf(ligne, "E %d %63s", &ecran, contenu) == 2 &&
            ecran >= 0 && ecran < N_SCREENS) {
            strcpy(video, contenu);
            serveur.en_cours[ecran] = video;
            printf("[RESTAURATION écran %d] %s à reprendre\n", ecran, video);
        } else if (strncmp(ligne, "Q ", 2) == 0) {
            strcpy(video, ligne + 2);
            if (!deposer(video)) free(video);
        } else {
            fprintf(stderr, "Enregistrement de sauvegarde invalide\n");
            free(video);
            exit(1);
        }
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
        serveur_secondaire(); // Lance le serveur secondaire
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
    int ids_ecrans[N_SCREENS];
    for (int e = 0; e < N_SCREENS; e++){
        ids_ecrans[e] = e; // Initialise le numéro de chaque écran pour le transmettre à son thread

    }
     
    int i;

    for (i = 0; i < N_CAMERAS; i++)
        pthread_create(&cameras[i], NULL, boucle_camera, NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_create(&ecrans[i], NULL, boucle_ecran, &ids_ecrans[i]);
    pthread_create(&panne, NULL, surveiller_panne, NULL);

    for (i = 0; i < N_CAMERAS; i++)
        pthread_join(cameras[i], NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_join(ecrans[i], NULL);
    pthread_join(panne, NULL);

    // Affiche le nombre de vidéos restantes dans le buffer(celle pas visionné)
    printf("Tampon figé (%d vidéos) :\n", serveur.nb_videos);
    // Parcourt les vidéos restantes dans leur ordre de retrait
    for (i = 0; i < serveur.nb_videos; i++) {
        // Calcule la case à lire en revenant au début du buffer si nécessaire
        int indice = (serveur.indice_retrait + i) % BUFFER_SIZE;
        printf("  %s\n", serveur.videos[indice]);
    }

    envoyer_copie(pid_secondaire);

    printf("Appuyez sur Entree pour redemarrer le serveur principal.\n");
    fflush(stdout);
    char touche[16];
    fgets(touche, sizeof(touche), stdin); // on attends la touche entrée pour continuer

    breakdown_flag = 0; /* La panne simulée est résolue. */
    reinitialiser_tampon();
    recevoir_copie(); // Restaure les données sauvegardées sur le secondaire
    waitpid(pid_secondaire, NULL, 0);

    for (i = 0; i < N_CAMERAS; i++)
        pthread_create(&cameras[i], NULL, boucle_camera, NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_create(&ecrans[i], NULL, boucle_ecran, &ids_ecrans[i]);

    for (i = 0; i < N_CAMERAS; i++)
        pthread_join(cameras[i], NULL);
    for (i = 0; i < N_SCREENS; i++)
        pthread_join(ecrans[i], NULL);

    return 0;
}
