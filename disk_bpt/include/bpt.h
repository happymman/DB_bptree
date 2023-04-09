#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <string.h>
#define LEAF_MAX 4
#define INTERNAL_MAX 248

typedef struct record{ //리프노드 키+값
    int64_t key;
    char value[120];
}record;
// i 1 key1 -> insert [key] [value]

typedef struct inter_record { //internal노드 키+포인터
    int64_t key; //키값(이정표 역할)
    off_t p_offset; //자식노드 포인터
}I_R;

typedef struct Page{ //디스크버전에선 페이지가 노드
    off_t parent_page_offset; //부모노드 포인터
    int is_leaf; //leaf 여부
    int num_of_keys; //가지고 있는 키개수
    char reserved[104]; //페이지 여분
    off_t next_offset; //마지막 포인터
    union{
        I_R b_f[248]; //Disk-Internal노드 최대키 248 by페이지 크기 4K
        record records[31]; //Disk-leaf노드 최대키 31개 by페이지 크기 4K
    };
}page;

typedef struct Header_Page{ //메타페이지. 페이지전체에 관한 정보를 가지고 있는
    off_t fpo; 
    off_t rpo; //루트노드 포인터
    off_t recent_created; 
    off_t recent_splited; 
    int64_t num_of_pages;
    char reserved[4056];
}H_P;

//extern : 외부파일에서 정의한 변수를 선언 사용하겠다
extern int fd;

extern page * rt;

extern H_P * hp;
// FUNCTION PROTOTYPES.
int open_table(char * pathname);
H_P * load_header(off_t off);
page * load_page(off_t off);

void reset(off_t off);
off_t new_page();
off_t find_leaf(int64_t key);
char * db_find(int64_t key);
void freetouse(off_t fpo);
int cut(int length);
int parser();

void start_new_file(record rec);
int db_insert(int64_t key, char * value);
off_t insert_into_leaf(off_t leaf, record inst);
off_t insert_into_leaf_as(off_t leaf, record inst);
off_t insert_into_parent(off_t old, int64_t key, off_t newp);
int get_left_index(off_t left);
off_t insert_into_new_root(off_t old, int64_t key, off_t newp);
off_t insert_into_internal(off_t bumo, int left_index, int64_t key, off_t newp);
off_t insert_into_internal_as(off_t bumo, int left_index, int64_t key, off_t newp);

int db_delete(int64_t key);
void delete_entry(int64_t key, off_t deloff);
void redistribute_pages(off_t need_more, int nbor_index, off_t nbor_off, off_t par_off, int64_t k_prime, int k_prime_index);
void coalesce_pages(off_t will_be_coal, int nbor_index, off_t nbor_off, off_t par_off, int64_t k_prime);
void adjust_root(off_t deloff);
void remove_entry_from_page(int64_t key, off_t deloff);
void usetofree(off_t wbf);

#endif /* __BPT_H__*/


