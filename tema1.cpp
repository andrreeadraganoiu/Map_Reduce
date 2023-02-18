#include <iostream>
#include<fstream>
#include <vector>
#include<math.h>
#include <pthread.h>
#include <bits/stdc++.h>

using namespace std;

struct my_arg {
	int id;
    int num_reducers;
    int num_mappers;
	pthread_barrier_t *barrier;
    pthread_mutex_t *mutex_del;  /* mutex-ul care se ocupa ca 2 thread-uri 
                                    sa nu stearga un fisier in acelasi timp */
    pthread_mutex_t *mutex_add;  /* mutex-ul care se ocupa ca 2 thread-uri 
                                    sa nu adauge intr-un vector in acelasi timp */
    vector<string> *in_files;
    vector<vector<unordered_set<int>>> *res; /* vector de vectori care contin seturile de puteri ale unui mapper 
                                                (vectorul final contine deci toate seturilor tuturor mapperilor) */   
};


int binary_search(int left, int right,int k, int x) 
{       
        while (left <= right) 
        {
            int mid = (left + right) / 2;          
            int p = pow(mid, k);

            if (x == p) return x; 
            else if (x < p || p < 0) right = mid - 1; /* daca ridicare la putere este mai mare decat numarul citit, 
                                                          sau e atat de mare incat se face overflow si devine negativ */
            else if (x > p) left = mid + 1; 
        }
 
        return -1;                            
}

void read_in_files(string header_file, vector<string> &in_files)
{
    int num_files;
    string file_name;
    
    ifstream f(header_file);
    f>>num_files;
    
    while(f>>file_name)
    {
        in_files.push_back(file_name);
    }
    f.close();
}

void *f_map(void *arg)
{
    struct my_arg* data = (struct my_arg*) arg;
    
    int nr, x;
    string file;
    /* vector cu seturile de puteri pentru un thread map */
    vector<unordered_set<int>> set(data->num_reducers + 1);

    /* cat timp mai sunt fisiere */
    while(data->in_files->size() > 0)
    {
        /* pun lock pe regiunea in care se sterge din vector fisierul ales, 
        pentru a nu-l sterge mai multe thread-uri*/
        pthread_mutex_lock(data->mutex_del);

        file = data->in_files->back(); 
        data->in_files->pop_back();    

        pthread_mutex_unlock(data->mutex_del);
       
        ifstream f(file);
        f>>nr;

        while(f>>x)
        {
            for(int k = 0; k <= data->num_reducers - 1; k++)       
            {
                int res = binary_search(1, sqrt(x) + 1, k + 2, x);     
                if(res != -1)
                {
                    set[k].insert(res);
                }
            }
        }
        f.close();
        /* pun lock pe regiunea in care se adauga seturile unui thread intr-un vector final, 
        pentru a nu adauga mai multe thread-uri in acelasi timp */
        pthread_mutex_lock(data->mutex_add);
        data->res->push_back(set);
        pthread_mutex_unlock(data->mutex_add);
    }
    pthread_barrier_wait(data->barrier);

    pthread_exit(NULL);
}

void *f_reduce(void *arg)
{
    struct my_arg* data = (struct my_arg*) arg;
    pthread_barrier_wait(data->barrier);

    int id = data->id - data->num_mappers;
    unordered_set<int> final_set;

    for (auto& map_result : *(data->res)) {
        for (int n : map_result[id]) {
            final_set.insert(n);
        }
    }

    ofstream g("out" + to_string(id + 2) + ".txt");
    g<<final_set.size();
    g.close();
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) 
{

    int num_mappers = atoi(argv[1]);
    int num_reducers = atoi(argv[2]);

    /* numele fisierului care contine fisierele de intrare */ 
    string header_file = argv[3];

    int num_threads = num_mappers + num_reducers;

    /* vector cu toate thread-urile */
    pthread_t threads[num_threads];

    /* vector de structuri cu informatiile necesare fiecare thread */
    struct my_arg arguments[num_threads];

    /* alocare statica pentru bariera*/
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, num_threads);
    
    /* alocare statica pentru mutex-ul care se ocupa ca 2 thread-uri 
    sa nu stearga un fisier in acelasi timp */
    pthread_mutex_t mutex_del;
    pthread_mutex_init(&mutex_del, NULL);

    /* alocare statica pentru mutex-ul care se ocupa ca 2 thread-uri 
    sa nu adauge intr-un vector in acelasi timp */
    pthread_mutex_t mutex_add;
    pthread_mutex_init(&mutex_add, NULL);

    vector<vector<unordered_set<int>>> res;

    /* vector cu numele fisirelor ce trebuie citite*/
    vector<string> in_files;
    read_in_files(header_file, in_files);
    
    void *status;
    int r;

    for(int i = 0; i < num_threads; i++)
    {
        arguments[i].id = i;
        arguments[i].num_mappers = num_mappers;
        arguments[i].num_reducers = num_reducers;
        arguments[i].barrier = &barrier;
        arguments[i].mutex_del= &mutex_del;
        arguments[i].mutex_add = &mutex_add;
        arguments[i].res = &res;
        arguments[i].in_files = &in_files;

        if(i < num_mappers)
        {
            r = pthread_create(&threads[i], NULL, f_map, &arguments[i]);
        }
        else
        {
            r = pthread_create(&threads[i], NULL, f_reduce, &arguments[i]);
        }
        if (r) 
        {
            exit(-1);
        }
    }

    for (int i = 0; i < num_threads; i++) 
    {
		int r = pthread_join(threads[i], &status);

		if (r) 
        {
			exit(-1);
		}
	}

    pthread_mutex_destroy(&mutex_del);
    pthread_mutex_destroy(&mutex_add);
    pthread_barrier_destroy(&barrier);

    return 0;
}
