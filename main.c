#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>


#define TASK_DATA_SIZE 1024
#define MAX_WORKERS 7
#define MAX_DESCRIPTORS 8
#define MAX_QUEUE_SIZE 50
#define EPOLL_EVENTS 10

typedef struct{
  	int id;
  	char data[TASK_DATA_SIZE];
} Task;

typedef struct{
  	Task tasks[MAX_QUEUE_SIZE];
  	int head;
  	int tail;
  	pthread_mutex_t mu;
  	pthread_cond_t full_cond;
  	pthread_cond_t empty_cond;
} TastQueue;

typedef struct{
  	int id;
  	TastQueue *que;
} Worker;

typedef struct{
  	int pipe_fd[2];
  	pthread_mutex_t mu;
} Descriptor;

Worker workers[MAX_WORKERS];
TastQueue queues[MAX_WORKERS];
Descriptor descriptors[MAX_DESCRIPTORS]; 
int n_descriptors = 0;
int epollfd;


void queue_init(TastQueue* que){
  	que->head = 0;
  	que->tail = 0;
  	pthread_mutex_init(&que->mu, NULL);
  	pthread_cond_init(&que->full_cond, NULL);
  	pthread_cond_init(&que->empty_cond, NULL);
}

void queue_push(TastQueue* que, Task task){
  	pthread_mutex_lock(&que->mu);
  
  	while((que->tail + 1) % MAX_QUEUE_SIZE == que->head)
    	pthread_cond_wait(&que->full_cond, &que->mu);

 		que->tasks[que->tail] = task;
  	que->tail = (que->tail + 1) % MAX_QUEUE_SIZE;
  	pthread_cond_signal(&que->empty_cond);
  
  	pthread_mutex_unlock(&que->mu);
}

Task queue_pop(TastQueue* que){
  	pthread_mutex_lock(&que->mu);
  	
  	Task task;
  	
  	while(que->head == que->tail)
    		pthread_cond_wait(&que->empty_cond, &que->mu);

  	task = que->tasks[que->head];
  	que->head = (que->head + 1) % MAX_QUEUE_SIZE;
  	pthread_cond_signal(&que->full_cond);
  	pthread_mutex_unlock(&que->mu);
  	
  	return task;
}

void create_pipe(int pipefd[2]){
  	if(pipe(pipefd) == -1){
    	perror("pipe");
    	exit(1);
  	}
}

void* task_setter(void* arg){
  	while(1){
	  		srand(time(NULL));
	    	int rand_fd_id = rand() % MAX_DESCRIPTORS;
	    	int sleep_time = (rand() + 500000) % 1000000;
	    	int res = write(descriptors[rand_fd_id].pipe_fd[1], "abcd", 5);
	    	if(res == -1) {
	      		perror("write");
	      		exit(1);
	    	}
	    	usleep(sleep_time);
  	}
}

void* master_thread(void* arg){
  	Task task;
  	char buffer[TASK_DATA_SIZE];
  	struct epoll_event events[EPOLL_EVENTS];

  	epollfd = epoll_create(5000);
		if(epollfd == -1){
	    	perror("epoll_create");
	    	exit(1);
  	}

  	for(int i = 0; i < n_descriptors; ++i){  //Add descriptors in epoll
	    	struct epoll_event event;
	    	event.events = EPOLLIN;
	    	event.data.fd = descriptors[i].pipe_fd[0];  // descriptor for read
	    	if(epoll_ctl(epollfd, EPOLL_CTL_ADD, descriptors[i].pipe_fd[0], &event) == -1){
	      		perror("epoll_ctl");
	      		exit(1);
	    	}
  	}

  	while(1){
		    int nfds = epoll_wait(epollfd, events, EPOLL_EVENTS, -1);
		    printf("Epoll give %d starts\n", nfds); //amounts of fds to read
		    if(nfds == -1){
		      perror("epoll_wait");
		      exit(1);
		    } 
		    else if(nfds == 0){
		      perror("epoll_wait_timeout");
		      exit(1);
		    }

		    for(int i = 0; i < nfds; i++){ //Обработка events
		      	if(events[i].events & EPOLLIN){
			        	int fd = events[i].data.fd;
			        	if(read(fd, buffer, sizeof(buffer)) > 0){ //read data
				        		srand(time(NULL));
				         	 	task.id = (rand() + fd) % 100;
				          	strcpy(task.data, buffer);

				          	srand(time(NULL));
						        int rand1 = rand() % MAX_WORKERS;
						        int rand2 = rand() % MAX_WORKERS;
						          
						        //Choose worker with smaller queue
						        Worker *worker1 = &workers[rand1];
						        Worker *worker2 = &workers[rand2];
						        if(worker1->que->tail - worker1->que->head < worker2->que->tail - worker2->que->head){
						            queue_push(worker1->que, task);
						            printf("Master sent task %d to worker %d\n", task.id, worker1->id);
						        } 
						        else{
						            queue_push(worker2->que, task);
						            printf("Master sent task %d to worker %d\n", task.id, worker2->id);
						        }
			        	}
		    		}
	 			}
		}	
	    
  	pthread_exit(NULL);
}


void* worker_thread(void* arg){
  	Worker* worker = (Worker*)arg;
  	Task task;
  	
  	printf("Worker %d starts\n", worker->id);

  	while (1){
    	if(worker->que->tail != worker->que->head){
	      	task = queue_pop(worker->que);
	      	printf("Worker %d is doing task %d\n", worker->id, task.id);
	      	
	      	int sleep_time = (rand() + 500000) % 1000000;
	      	usleep(sleep_time);
	      	printf("Worker %d is doing task %d\n", worker->id, task.id);
	      	continue;
    	}

    	
    	//Queue is empty, try to "steal" task from another worker
      	
      	srand(time(NULL));
      	int rand1 = rand() % MAX_WORKERS;
      	int rand2 = rand() % MAX_WORKERS;
      	while(rand1 == worker->id || rand2 == worker->id){
        	rand1 = rand() % MAX_WORKERS;
        	rand2 = rand() % MAX_WORKERS;
      	}
      
      	//Choose worker with bigger queue
      	Worker *thief1 = &workers[rand1];
      	Worker *thief2 = &workers[rand2];
      	if(thief1->que->tail - thief1->que->head > thief2->que->tail - thief2->que->head ||
 					thief2->que->tail == thief2->que->head){
        	task = queue_pop(thief1->que);
        	printf("Worker %d has stollen task %d from worker's %d queue\n", worker->id, task.id, thief1->id);
      	} 
      	else if(thief2->que->tail != thief2->que->head){
        	task = queue_pop(thief2->que);
        	printf("Worker %d has stollen task %d from worker's %d queue\n", worker->id, task.id, thief2->id);
      	} 
      	else{
        	continue;
      	}

      	printf("Worker %d is doing task %d\n", worker->id, task.id);

      	int sleep_time = (rand() + 500000) % 1000000;
      	usleep(sleep_time);
      	printf("Worker %d has done %d\n", worker->id, task.id);
    
  	}

  	pthread_exit(NULL);
}


int main(){
  	pthread_t master_thread_id;
  	pthread_t worker_threads[MAX_WORKERS];
  	pthread_t task_setter_id;

  	for(int i = 0; i < MAX_WORKERS; ++i){
    	queue_init(&queues[i]);
    	workers[i].id = i;
    	workers[i].que = &queues[i];
  	}

  	for(int i = 0; i < MAX_DESCRIPTORS; ++i){
    	create_pipe(descriptors[i].pipe_fd);
    	pthread_mutex_init(&(descriptors[i].mu), NULL);
    	printf("fd id = %d pipe_fd[0] = %d pipe_fd[1] = %d\n", i, descriptors[i].pipe_fd[0], descriptors[i].pipe_fd[1]);
    	n_descriptors++;
  	}

  	if(pthread_create(&master_thread_id, NULL, master_thread, NULL) != 0){
    	perror("pthread_create");
    	return 1;
  	}

  	for(int i = 0; i < MAX_WORKERS; i++){
    	if(pthread_create(&worker_threads[i], NULL, worker_thread, &workers[i]) != 0){
      		perror("pthread_create");
      		return 2;
    	}
  	}

  	if(pthread_create(&task_setter_id, NULL, task_setter, NULL) != 0){
    	perror("pthread_create_task_setter");
    	return 3;
  	}

  	pthread_join(task_setter_id, NULL);
  	pthread_join(master_thread_id, NULL);
  	for(int i = 0; i < MAX_WORKERS; i++){
    	pthread_join(worker_threads[i], NULL);
  	}

  	for(int i = 0; i < MAX_WORKERS; i++){
    	pthread_mutex_destroy(&queues[i].mu);
    	pthread_cond_destroy(&queues[i].empty_cond);
    	pthread_cond_destroy(&queues[i].full_cond);
  	}

  	for(int i = 0; i < n_descriptors; i++){
    	close(descriptors[i].pipe_fd[0]);
    	close(descriptors[i].pipe_fd[1]);
    	pthread_mutex_destroy(&(descriptors[i].mu));
  	}

  	close(epollfd);

  return 0;
}
