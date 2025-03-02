#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

typedef struct {
    int rank;

    int num_files;
    char file_names[MAX_FILES][MAX_FILENAME];
    int file_lines_num[MAX_FILES];
    char file_data[MAX_FILES][MAX_CHUNKS][HASH_SIZE + 1];

    int wanted_num_files;
    char wanted_files[MAX_FILES][MAX_FILENAME];
    int num_seg[MAX_FILES];
} data;

void *download_thread_func(void *arg)
{
    data arg_data = *(data*)arg;
    int rank = arg_data.rank;

    char message[HASH_SIZE + 1];
    char message_reply[HASH_SIZE + 1];
    int reply[32];
    int current_file = 0;

    // if we don't want to download files let the traker know
    if (arg_data.wanted_files[current_file] == 0) {
        strcat(message, "no dowload");
        MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        return NULL;
    }

    int num_seeders;
    int num_peers;
    int idx_peers = 0;
    int seeders[MAX_FILES];
    int peers[MAX_FILES];

    int src_rank;

    while (current_file < arg_data.wanted_num_files) {
        if (arg_data.num_seg[current_file] == 0) {
            // Dowload new file

            // get the hashes for the file
            sprintf(message, "hash %s", arg_data.wanted_files[current_file]);
            MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
            MPI_Recv(&arg_data.file_lines_num[arg_data.num_files + current_file], 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD, NULL);
            for (int i = 0; i < arg_data.file_lines_num[arg_data.num_files + current_file]; i++) {
                MPI_Recv(arg_data.file_data[arg_data.num_files + current_file][i], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD, NULL);
            }

            // get the swarm
            memset(message, 0, sizeof(message));
            memset(reply, 0, sizeof(reply));
            sprintf(message, "list %s", arg_data.wanted_files[current_file]);

            MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
            MPI_Recv(reply, 32, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD, NULL);

            num_seeders = reply[0];
            for (int i = 0; i < num_seeders; i++) {
                seeders[i] = reply[i + 1];
            }
            num_peers = reply[num_seeders + 2];
            for (int i = 0; i < num_peers; i++) {
                peers[i] = reply[i + num_peers + 2];
            }

            if (num_peers >= 1) {
                for (idx_peers = 0; idx_peers < num_peers; idx_peers++) {
                    if (peers[idx_peers] != rank) {
                        src_rank = peers[idx_peers];
                        idx_peers++;
                        break;
                    }
                }
                src_rank = seeders[0];
            } else {
                src_rank = seeders[0];
            }
            
        } else if (arg_data.num_seg[current_file] % 10 == 0) {
            // After receiving 10 segments request the swarm again
            memset(message, 0, sizeof(message));
            memset(reply, 0, sizeof(reply));
            sprintf(message, "list %s", arg_data.wanted_files[current_file]);

            MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
            MPI_Recv(reply, 32, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD, NULL);

            memset(seeders, 0, sizeof(seeders));
            memset(peers, 0, sizeof(peers));

            num_seeders = reply[0];
            for (int i = 0; i < num_seeders; i++) {
                seeders[i] = reply[i + 1];
            }
            num_peers = reply[num_seeders + 2];
            for (int i = 0; i < num_peers; i++) {
                peers[i] = reply[i + num_peers + 2];
            }


            if (num_peers >= 1) {
                for (idx_peers = 0; idx_peers < num_peers; idx_peers++) {
                    if (peers[idx_peers] != rank) {
                        src_rank = peers[idx_peers];
                        idx_peers++;
                        break;
                    }
                }
                src_rank = seeders[0];
            } else {
                src_rank = seeders[0];
            }
        }

        memset(message, 0, sizeof(message));
        memset(reply, 0, sizeof(reply));

        sprintf(message, "down %s %d", arg_data.wanted_files[current_file], arg_data.num_seg[current_file]);
        // We use tag 1 to send download request and tag 2 to get the response

        // Simulate the download. Send the request to a client for the file and wait for the ACK
        MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, src_rank, 1, MPI_COMM_WORLD);
        MPI_Recv(message_reply, HASH_SIZE + 1, MPI_CHAR, src_rank, 2, MPI_COMM_WORLD, NULL);

        // If we got an NACK try sending requests until you get the file segment
        while (strcmp(message_reply, "ACK") != 0) {

            if (idx_peers < num_peers) {
                src_rank = peers[idx_peers];
                idx_peers++;
            } else {
                src_rank = seeders[0];
            }

            memset(message, 0, sizeof(message));
            memset(message_reply, 0, sizeof(reply));

            sprintf(message, "down %s %d", arg_data.wanted_files[current_file], arg_data.num_seg[current_file]);
            MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, src_rank, 1, MPI_COMM_WORLD);
            MPI_Recv(message_reply, HASH_SIZE + 1, MPI_CHAR, src_rank, 2, MPI_COMM_WORLD, NULL);
        }

        arg_data.num_seg[current_file]++;

        if (arg_data.num_seg[current_file] == arg_data.file_lines_num[arg_data.num_files + current_file]) {
            // File finished dowloading
            memset(message, 0, sizeof(message));
            sprintf(message, "done %s", arg_data.wanted_files[current_file]);
            MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, src_rank, 0, MPI_COMM_WORLD);

            // Write to file
            FILE* fd;
            char file_name[FILENAME_MAX];
            memset(file_name, 0, sizeof(file_name));
            sprintf(file_name, "client%d_%s", rank, arg_data.wanted_files[current_file]);

            fd = fopen(file_name, "w");

            for (int i = 0; i < arg_data.num_seg[current_file]; i++) {
                fprintf(fd, "%s", arg_data.file_data[arg_data.num_files + current_file][i]);
                if (i < arg_data.num_seg[current_file] - 1) {
                    fprintf(fd, "\n");
                }
            }

            fclose(fd);

            current_file++;
        }
    }

    // if we leave the loop it means we finished downloading all the wanted files.
    memset(message, 0, sizeof(message));
    strcpy(message, "finish");
    MPI_Send(message, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    data arg_data = *(data*)arg;

    char message[HASH_SIZE + 1];
    char message_reply[HASH_SIZE + 1];
    char file[FILENAME_MAX];
    int seg;

    while (1) {
        MPI_Status status;
        memset(message, 0, sizeof(message));
        MPI_Recv(message, HASH_SIZE + 1, MPI_CHAR, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);

        // download request
        if (strncmp(message, "down", 4) == 0) {
            memset(file, 0, sizeof(file));
            sscanf(message + 5, "%s", file);
            sscanf(message + 5 + strlen(file), "%d", &seg);

            int ok = 0;
            // first check file_names which contains files uploaded by us, so we already have all the segments
            for (int i = 0; i < arg_data.num_files; i++) {
                if (strcmp(arg_data.file_names[i], file) == 0) {
                    memset(message_reply, 0, sizeof(message_reply));
                    strcpy(message_reply, "ACK");
                    MPI_Send(message_reply, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 2, MPI_COMM_WORLD);

                    ok = 1;
                    break;
                }
            }

            if (ok == 1) {
                continue;
            }

            // check the downloaded files
            for (int i = 0; i < arg_data.wanted_num_files; i++) {
                if (strcmp(arg_data.wanted_files[i], file) == 0) {
                    // check if the segment is available
                    if (arg_data.file_lines_num[arg_data.num_files + i] >= seg) {
                        memset(message_reply, 0, sizeof(message_reply));
                        strcpy(message_reply, "ACK");
                        MPI_Send(message_reply, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 2, MPI_COMM_WORLD);
                        
                        ok = 1;
                        break;
                    } else {
                        break;
                    }
                }
            }

            if (ok == 1) {
                continue;
            }

            // send nack if the segments isn't available
            memset(message_reply, 0, sizeof(message_reply));
            strcpy(message_reply, "NACK");
            MPI_Send(message_reply, HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 2, MPI_COMM_WORLD);

        }

        // all the clients have finished downloading, stop execution
        if (strcmp(message, "finish") == 0) {
            break;
        }
    }

    return NULL;
}

void tracker(int numtasks, int rank) {
    char file_name[MAX_FILES][MAX_FILENAME];
    int file_lines[MAX_FILES];  // how many chunks each file has
    char file_data[MAX_FILES][MAX_CHUNKS][HASH_SIZE + 1];
    int seeders[MAX_FILES][numtasks];
    int seeders_num[MAX_FILES];
    int peers[MAX_FILES][numtasks];
    int peers_num[MAX_FILES];

    int file_idx = 0;
    int num_files = 0;
    for (int i = 0; i < MAX_FILES; i++) {
        peers_num[i] = 0;
    }
    
    int num_clients = numtasks - 1;

    // Init
    for (int i = 0; i < num_clients; i++) {
        MPI_Status status;
        int num_files_recv;

        // Start receiving init data from a client
        MPI_Recv(&num_files_recv, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        for (int i = 0; i < num_files_recv; i++) {
            MPI_Recv(&file_name[file_idx], MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, NULL);
            seeders[file_idx][0] = status.MPI_SOURCE;
            seeders_num[file_idx] = 1;

            MPI_Recv(&file_lines[file_idx], 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, NULL);
            for (int j = 0; j < file_lines[file_idx]; j++) {
                MPI_Recv(file_data[file_idx][j], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, NULL);
            }
            file_idx++;
        }

        num_files += num_files_recv;
    }

    char ack[4] = "ack";
    MPI_Bcast(ack, 4, MPI_CHAR, TRACKER_RANK, MPI_COMM_WORLD);


    // wait for messages
    char message[HASH_SIZE + 1];
    char message_reply[HASH_SIZE + 1];
    int reply[32];
    char file[MAX_FILENAME];

    int num_finished = 0;
    while (1) {
        MPI_Status status;
        MPI_Recv(message, HASH_SIZE + 1, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        // send the hashes to client that wants to download the file
        if (strncmp(message, "hash", 4) == 0) {
            memset(file, 0, sizeof(file));
            strcpy(file, message + 5);

            for (int i = 0; i < num_files; i++) {
                if (strcmp(file_name[i], file) == 0) {
                    MPI_Send(&file_lines[i], 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);

                    for (int j = 0; j < file_lines[i]; j++) {
                        MPI_Send(file_data[i][j], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
                    }
                    break;

                    // add client to peers
                    peers[i][peers_num[i]] = status.MPI_SOURCE;
                    peers_num[i]++;
                }
            }


        }

        // send the swarm of peers/seeders
        if (strncmp(message, "list", 4) == 0) {
            strcpy(file, message + 5);

            memset(reply, 0, sizeof(reply));

            // find the file
            for (int i = 0; i < num_files; i++) {
                if (strcmp(file_name[i], file) == 0) {
                    reply[0] = seeders_num[i];
                    for (int j = 0, idx = 1; j < seeders_num[i]; j++, idx++) {
                        reply[idx] = seeders[i][0];
                    }

                    reply[seeders_num[i] + 1] = peers_num[i];
                    for (int j = 0, idx = 1 + seeders_num[i]; j < peers_num[i]; j++, idx++) {
                        reply[idx] = peers[i][0];
                    }

                    // the client will always try the first the first peer/seeder,
                    // so to balance things we'll send the first to the back of the list with each list request
                    // we can also avoid a client being used more than others
                    int aux = seeders[i][0];
                    for (int j = 0; j < seeders_num[i] - 1; j++) {
                        seeders[i][j] = seeders[i][j + 1];
                    }
                    seeders[i][seeders_num[i] - 1] = aux;

                    // add the client to peers list if it isn't already
                    int not_in_peers = 1;
                    for (int j = 0; j < peers_num[i]; j++) {
                        if (peers[i][j] == status.MPI_SOURCE) {
                            not_in_peers = 0;
                            break;
                        }
                    }
                    if (not_in_peers) {
                        peers[i][peers_num[i]] = status.MPI_SOURCE;
                        peers_num[i]++;
                    }

                    aux = peers[i][0];
                    for (int j = 0; j < peers_num[i] - 1; j++) {
                        peers[i][j] = peers[i][j + 1];
                    }
                    peers[i][peers_num[i] - 1] = aux;

                    break;
                }
            }

            MPI_Send(reply, 32, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
        }

        // file finished downloading
        if (strncmp(message, "done", 4) == 0) {
            memset(file, 0, sizeof(file));
            sscanf(message + 5, "%s", file);

            for (int i = 0; i < num_files; i++) {
                if (strcmp(file_name[i], file) == 0) {
                    // remove client from peers and add to seeders

                    for (int j = 0; j < peers_num[i]; i++) {
                        if (peers[i][j] == status.MPI_SOURCE) {
                            for (int k = j; k < peers_num[i] - 1; j++) {
                                peers[i][k] = peers[i][k + 1];
                            }
                            peers_num[i]--;
                            
                            break;
                        }
                    }

                    seeders[i][seeders_num[i]] = status.MPI_SOURCE;
                    seeders_num[i]++;
                }
            }
        }

        if (strcmp(message, "finish") == 0) {
            num_finished++;
        }

        // notify clients to stop uploading the files
        if (numtasks - 1 == num_finished) {
            memset(message_reply, 0, sizeof(message_reply));
            strcpy(message_reply, "finish");
            for (int i = 1 ; i < numtasks; i++) {
                MPI_Send(message_reply, HASH_SIZE + 1, MPI_CHAR, i, 1, MPI_COMM_WORLD);
            }
            break;
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    // Init
    char input_file[MAX_FILENAME];
    sprintf(input_file, "in%d.txt", rank);

    FILE* fptr;
    fptr = fopen(input_file, "r");
    if (fptr == NULL) {
        printf("File %s couldn't be opened.\n", input_file);
        return;
    }

    data arg_data;
    memset(&arg_data, 0, sizeof(data));
    arg_data.rank = rank;

    // Read data from files
    fscanf(fptr, "%d", &arg_data.num_files);
    for (int i = 0; i < arg_data.num_files; i++) { 
        fscanf(fptr, "%s", arg_data.file_names[i]);
        fscanf(fptr, "%d", &arg_data.file_lines_num[i]);

        // Read hashes
        for (int j = 0; j < arg_data.file_lines_num[i]; j++) {
            fscanf(fptr, "%s", arg_data.file_data[i][j]);
        }
    }
    
    fscanf(fptr, "%d", &arg_data.wanted_num_files);
    for (int i = 0; i < arg_data.wanted_num_files; i++) {
        fscanf(fptr, "%s", arg_data.wanted_files[i]);
    }

    fclose(fptr);
    
    // Send init data to the tracker
    MPI_Send(&arg_data.num_files, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
    for (int i = 0; i < arg_data.num_files; i++) {
        MPI_Send(&arg_data.file_names[i], strlen(arg_data.file_names[i]), MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        MPI_Send(&arg_data.file_lines_num[i], 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        for (int j = 0; j < arg_data.file_lines_num[i]; j++) {
            MPI_Send(arg_data.file_data[i][j], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
    }
    
    // Wait for ack from Tracker for init data
    char recv_buf[4];
    MPI_Bcast(recv_buf, 4, MPI_CHAR, TRACKER_RANK, MPI_COMM_WORLD);

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &arg_data);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &arg_data);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }


}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
