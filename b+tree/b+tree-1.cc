// Deletion operation on a B+ Tree in C++
/*

https://www.programiz.com/dsa/deletion-from-a-b-plus-tree
*/

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_ORDER 3

typedef struct record {
  int value;
} record;

typedef struct node {
  void **pointers;
  int *keys;
  struct node *parent;
  bool is_leaf;
  int num_keys;
  struct node *next;
} node;

int order = DEFAULT_ORDER;
node *queue = NULL;
bool verbose_output = false;

void enqueue(node *new_node);
node *dequeue(void);
int height(node *const root);
int path_to_root(node *const root, node *child);
void print_leaves(node *const root);
void print_tree(node *const root);
void find_and_print(node *const root, int key, bool verbose);
void find_and_print_range(node *const root, int range1, int range2, bool verbose);
int find_range(node *const root, int key_start, int key_end, bool verbose,
         int returned_keys[], void *returned_pointers[]);
node *find_leaf(node *const root, int key, bool verbose);
record *find(node *root, int key, bool verbose, node **leaf_out);
int cut(int length);

record *make_record(int value);
node *make_node(void);
node *make_leaf(void);
int get_left_index(node *parent, node *left);
node *insert_into_leaf(node *leaf, int key, record *pointer);
node *insert_into_leaf_after_splitting(node *root, node *leaf, int key,
                     record *pointer);
node *insert_into_node(node *root, node *parent,
             int left_index, int key, node *right);
node *insert_into_node_after_splitting(node *root, node *parent,
                     int left_index,
                     int key, node *right);
node *insert_into_parent(node *root, node *left, int key, node *right);
node *insert_into_new_root(node *left, int key, node *right);
node *start_new_tree(int key, record *pointer);
node *insert(node *root, int key, int value);

int get_neighbor_index(node *n);
node *adjust_root(node *root);
node *coalesce_nodes(node *root, node *n, node *neighbor,
           int neighbor_index, int k_prime);
node *redistribute_nodes(node *root, node *n, node *neighbor,
             int neighbor_index,
             int k_prime_index, int k_prime);
node *delete_entry(node *root, node *n, int key, void *pointer);
node *delete (node *root, int key);

void enqueue(node *new_node) {
  node *c;
  if (queue == NULL) {
    queue = new_node;
    queue->next = NULL;
  } else {
    c = queue;
    while (c->next != NULL) {
      c = c->next;
    }
    c->next = new_node;
    new_node->next = NULL;
  }
}

node *dequeue(void) {
  node *n = queue;
  queue = queue->next;
  n->next = NULL;
  return n;
}

void print_leaves(node *const root) {
  if (root == NULL) {
    printf("Empty tree.\n");
    return;
  }
  int i;
  node *c = root;
  while (!c->is_leaf)
    c = c->pointers[0];
  while (true) {
    for (i = 0; i < c->num_keys; i++) {
      if (verbose_output)
        printf("%p ", c->pointers[i]);
      printf("%d ", c->keys[i]);
    }
    if (verbose_output)
      printf("%p ", c->pointers[order - 1]);
    if (c->pointers[order - 1] != NULL) {
      printf(" | ");
      c = c->pointers[order - 1];
    } else
      break;
  }
  printf("\n");
}

int height(node *const root) {
  int h = 0;
  node *c = root;
  while (!c->is_leaf) {
    c = c->pointers[0];
    h++;
  }
  return h;
}
int path_to_root(node *const root, node *child) {
  int length = 0;
  node *c = child;
  while (c != root) {
    c = c->parent;
    length++;
  }
  return length;
}

void print_tree(node *const root) {
  node *n = NULL;
  int i = 0;
  int rank = 0;
  int new_rank = 0;

  if (root == NULL) {
    printf("Empty tree.\n");
    return;
  }
  queue = NULL;
  enqueue(root);
  while (queue != NULL) {
    n = dequeue();
    if (n->parent != NULL && n == n->parent->pointers[0]) {
      new_rank = path_to_root(root, n);
      if (new_rank != rank) {
        rank = new_rank;
        printf("\n");
      }
    }
    if (verbose_output)
      printf("(%p)", n);
    for (i = 0; i < n->num_keys; i++) {
      if (verbose_output)
        printf("%p ", n->pointers[i]);
      printf("%d ", n->keys[i]);
    }
    if (!n->is_leaf)
      for (i = 0; i <= n->num_keys; i++)
        enqueue(n->pointers[i]);
    if (verbose_output) {
      if (n->is_leaf)
        printf("%p ", n->pointers[order - 1]);
      else
        printf("%p ", n->pointers[n->num_keys]);
    }
    printf("| ");
  }
  printf("\n");
}

void find_and_print(node *const root, int key, bool verbose) {
  node *leaf = NULL;
  record *r = find(root, key, verbose, NULL);
  if (r == NULL)
    printf("Record not found under key %d.\n", key);
  else
    printf("Record at %p -- key %d, value %d.\n",
         r, key, r->value);
}

void find_and_print_range(node *const root, int key_start, int key_end,
              bool verbose) {
  int i;
  int array_size = key_end - key_start + 1;
  int returned_keys[array_size];
  void *returned_pointers[array_size];
  int num_found = find_range(root, key_start, key_end, verbose,
                 returned_keys, returned_pointers);
  if (!num_found)
    printf("None found.\n");
  else {
    for (i = 0; i < num_found; i++)
      printf("Key: %d   Location: %p  Value: %d\n",
           returned_keys[i],
           returned_pointers[i],
           ((record *)
            returned_pointers[i])
             ->value);
  }
}

int find_range(node *const root, int key_start, int key_end, bool verbose,
         int returned_keys[], void *returned_pointers[]) {
  int i, num_found;
  num_found = 0;
  node *n = find_leaf(root, key_start, verbose);
  if (n == NULL)
    return 0;
  for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++)
    ;
  if (i == n->num_keys)
    return 0;
  while (n != NULL) {
    for (; i < n->num_keys && n->keys[i] <= key_end; i++) {
      returned_keys[num_found] = n->keys[i];
      returned_pointers[num_found] = n->pointers[i];
      num_found++;
    }
    n = n->pointers[order - 1];
    i = 0;
  }
  return num_found;
}

node *find_leaf(node *const root, int key, bool verbose) {
  if (root == NULL) {
    if (verbose)
      printf("Empty tree.\n");
    return root;
  }
  int i = 0;
  node *c = root;
  while (!c->is_leaf) {
    if (verbose) {
      printf("[");
      for (i = 0; i < c->num_keys - 1; i++)
        printf("%d ", c->keys[i]);
      printf("%d] ", c->keys[i]);
    }
    i = 0;
    while (i < c->num_keys) {
      if (key >= c->keys[i])
        i++;
      else
        break;
    }
    if (verbose)
      printf("%d ->\n", i);
    c = (node *)c->pointers[i];
  }
  if (verbose) {
    printf("Leaf [");
    for (i = 0; i < c->num_keys - 1; i++)
      printf("%d ", c->keys[i]);
    printf("%d] ->\n", c->keys[i]);
  }
  return c;
}

record *find(node *root, int key, bool verbose, node **leaf_out) {
  if (root == NULL) {
    if (leaf_out != NULL) {
      *leaf_out = NULL;
    }
    return NULL;
  }

  int i = 0;
  node *leaf = NULL;

  leaf = find_leaf(root, key, verbose);

  for (i = 0; i < leaf->num_keys; i++)
    if (leaf->keys[i] == key)
      break;
  if (leaf_out != NULL) {
    *leaf_out = leaf;
  }
  if (i == leaf->num_keys)
    return NULL;
  else
    return (record *)leaf->pointers[i];
}

int cut(int length) {
  if (length % 2 == 0)
    return length / 2;
  else
    return length / 2 + 1;
}

record *make_record(int value) {
  record *new_record = (record *)malloc(sizeof(record));
  if (new_record == NULL) {
    perror("Record creation.");
    exit(EXIT_FAILURE);
  } else {
    new_record->value = value;
  }
  return new_record;
}

node *make_node(void) {
  node *new_node;
  new_node = malloc(sizeof(node));
  if (new_node == NULL) {
    perror("Node creation.");
    exit(EXIT_FAILURE);
  }
  new_node->keys = malloc((order - 1) * sizeof(int));
  if (new_node->keys == NULL) {
    perror("New node keys array.");
    exit(EXIT_FAILURE);
  }
  new_node->pointers = malloc(order * sizeof(void *));
  if (new_node->pointers == NULL) {
    perror("New node pointers array.");
    exit(EXIT_FAILURE);
  }
  new_node->is_leaf = false;
  new_node->num_keys = 0;
  new_node->parent = NULL;
  new_node->next = NULL;
  return new_node;
}

node *make_leaf(void) {
  node *leaf = make_node();
  leaf->is_leaf = true;
  return leaf;
}

int get_left_index(node *parent, node *left) {
  int left_index = 0;
  while (left_index <= parent->num_keys &&
       parent->pointers[left_index] != left)
    left_index++;
  return left_index;
}

node *insert_into_leaf(node *leaf, int key, record *pointer) {
  int i, insertion_point;

  insertion_point = 0;
  while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
    insertion_point++;

  for (i = leaf->num_keys; i > insertion_point; i--) {
    leaf->keys[i] = leaf->keys[i - 1];
    leaf->pointers[i] = leaf->pointers[i - 1];
  }
  leaf->keys[insertion_point] = key;
  leaf->pointers[insertion_point] = pointer;
  leaf->num_keys++;
  return leaf;
}

node *insert_into_leaf_after_splitting(node *root, node *leaf, int key, record *pointer) {
  node *new_leaf;
  int *temp_keys;
  void **temp_pointers;
  int insertion_index, split, new_key, i, j;

  new_leaf = make_leaf();

  temp_keys = malloc(order * sizeof(int));
  if (temp_keys == NULL) {
    perror("Temporary keys array.");
    exit(EXIT_FAILURE);
  }

  temp_pointers = malloc(order * sizeof(void *));
  if (temp_pointers == NULL) {
    perror("Temporary pointers array.");
    exit(EXIT_FAILURE);
  }

  insertion_index = 0;
  while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
    insertion_index++;

  for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
    if (j == insertion_index)
      j++;
    temp_keys[j] = leaf->keys[i];
    temp_pointers[j] = leaf->pointers[i];
  }

  temp_keys[insertion_index] = key;
  temp_pointers[insertion_index] = pointer;

  leaf->num_keys = 0;

  split = cut(order - 1);

  for (i = 0; i < split; i++) {
    leaf->pointers[i] = temp_pointers[i];
    leaf->keys[i] = temp_keys[i];
    leaf->num_keys++;
  }

  for (i = split, j = 0; i < order; i++, j++) {
    new_leaf->pointers[j] = temp_pointers[i];
    new_leaf->keys[j] = temp_keys[i];
    new_leaf->num_keys++;
  }

  free(temp_pointers);
  free(temp_keys);

  new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
  leaf->pointers[order - 1] = new_leaf;

  for (i = leaf->num_keys; i < order - 1; i++)
    leaf->pointers[i] = NULL;
  for (i = new_leaf->num_keys; i < order - 1; i++)
    new_leaf->pointers[i] = NULL;

  new_leaf->parent = leaf->parent;
  new_key = new_leaf->keys[0];

  return insert_into_parent(root, leaf, new_key, new_leaf);
}

node *insert_into_node(node *root, node *n,
             int left_index, int key, node *right) {
  int i;

  for (i = n->num_keys; i > left_index; i--) {
    n->pointers[i + 1] = n->pointers[i];
    n->keys[i] = n->keys[i - 1];
  }
  n->pointers[left_index + 1] = right;
  n->keys[left_index] = key;
  n->num_keys++;
  return root;
}

node *insert_into_node_after_splitting(node *root, node *old_node, int left_index,
                     int key, node *right) {
  int i, j, split, k_prime;
  node *new_node, *child;
  int *temp_keys;
  node **temp_pointers;

  temp_pointers = malloc((order + 1) * sizeof(node *));
  if (temp_pointers == NULL) {
    perror("Temporary pointers array for splitting nodes.");
    exit(EXIT_FAILURE);
  }
  temp_keys = malloc(order * sizeof(int));
  if (temp_keys == NULL) {
    perror("Temporary keys array for splitting nodes.");
    exit(EXIT_FAILURE);
  }

  for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
    if (j == left_index + 1)
      j++;
    temp_pointers[j] = old_node->pointers[i];
  }

  for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
    if (j == left_index)
      j++;
    temp_keys[j] = old_node->keys[i];
  }

  temp_pointers[left_index + 1] = right;
  temp_keys[left_index] = key;

  split = cut(order);
  new_node = make_node();
  old_node->num_keys = 0;
  for (i = 0; i < split - 1; i++) {
    old_node->pointers[i] = temp_pointers[i];
    old_node->keys[i] = temp_keys[i];
    old_node->num_keys++;
  }
  old_node->pointers[i] = temp_pointers[i];
  k_prime = temp_keys[split - 1];
  for (++i, j = 0; i < order; i++, j++) {
    new_node->pointers[j] = temp_pointers[i];
    new_node->keys[j] = temp_keys[i];
    new_node->num_keys++;
  }
  new_node->pointers[j] = temp_pointers[i];
  free(temp_pointers);
  free(temp_keys);
  new_node->parent = old_node->parent;
  for (i = 0; i <= new_node->num_keys; i++) {
    child = new_node->pointers[i];
    child->parent = new_node;
  }

  return insert_into_parent(root, old_node, k_prime, new_node);
}

node *insert_into_parent(node *root, node *left, int key, node *right) {
  int left_index;
  node *parent;

  parent = left->parent;

  if (parent == NULL)
    return insert_into_new_root(left, key, right);

  left_index = get_left_index(parent, left);

  if (parent->num_keys < order - 1)
    return insert_into_node(root, parent, left_index, key, right);

  return insert_into_node_after_splitting(root, parent, left_index, key, right);
}

node *insert_into_new_root(node *left, int key, node *right) {
  node *root = make_node();
  root->keys[0] = key;
  root->pointers[0] = left;
  root->pointers[1] = right;
  root->num_keys++;
  root->parent = NULL;
  left->parent = root;
  right->parent = root;
  return root;
}

node *start_new_tree(int key, record *pointer) {
  node *root = make_leaf();
  root->keys[0] = key;
  root->pointers[0] = pointer;
  root->pointers[order - 1] = NULL;
  root->parent = NULL;
  root->num_keys++;
  return root;
}

node *insert(node *root, int key, int value) {
  record *record_pointer = NULL;
  node *leaf = NULL;

  record_pointer = find(root, key, false, NULL);
  if (record_pointer != NULL) {
    record_pointer->value = value;
    return root;
  }

  record_pointer = make_record(value);

  if (root == NULL)
    return start_new_tree(key, record_pointer);

  leaf = find_leaf(root, key, false);

  if (leaf->num_keys < order - 1) {
    leaf = insert_into_leaf(leaf, key, record_pointer);
    return root;
  }

  return insert_into_leaf_after_splitting(root, leaf, key, record_pointer);
}

int get_neighbor_index(node *n) {
  int i;
  for (i = 0; i <= n->parent->num_keys; i++)
    if (n->parent->pointers[i] == n)
      return i - 1;

  printf("Search for nonexistent pointer to node in parent.\n");
  printf("Node:  %#lx\n", (unsigned long)n);
  exit(EXIT_FAILURE);
}

node *remove_entry_from_node(node *n, int key, node *pointer) {
  int i, num_pointers;
  i = 0;
  while (n->keys[i] != key)
    i++;
  for (++i; i < n->num_keys; i++)
    n->keys[i - 1] = n->keys[i];

  num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
  i = 0;
  while (n->pointers[i] != pointer)
    i++;
  for (++i; i < num_pointers; i++)
    n->pointers[i - 1] = n->pointers[i];

  n->num_keys--;

  if (n->is_leaf)
    for (i = n->num_keys; i < order - 1; i++)
      n->pointers[i] = NULL;
  else
    for (i = n->num_keys + 1; i < order; i++)
      n->pointers[i] = NULL;

  return n;
}

node *adjust_root(node *root) {
  node *new_root;

  if (root->num_keys > 0)
    return root;

  if (!root->is_leaf) {
    new_root = root->pointers[0];
    new_root->parent = NULL;
  }

  else
    new_root = NULL;

  free(root->keys);
  free(root->pointers);
  free(root);

  return new_root;
}

node *coalesce_nodes(node *root, node *n, node *neighbor, int neighbor_index, int k_prime) {
  int i, j, neighbor_insertion_index, n_end;
  node *tmp;

  if (neighbor_index == -1) {
    tmp = n;
    n = neighbor;
    neighbor = tmp;
  }

  neighbor_insertion_index = neighbor->num_keys;

  if (!n->is_leaf) {
    neighbor->keys[neighbor_insertion_index] = k_prime;
    neighbor->num_keys++;

    n_end = n->num_keys;

    for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
      neighbor->keys[i] = n->keys[j];
      neighbor->pointers[i] = n->pointers[j];
      neighbor->num_keys++;
      n->num_keys--;
    }

    neighbor->pointers[i] = n->pointers[j];

    for (i = 0; i < neighbor->num_keys + 1; i++) {
      tmp = (node *)neighbor->pointers[i];
      tmp->parent = neighbor;
    }
  }

  else {
    for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
      neighbor->keys[i] = n->keys[j];
      neighbor->pointers[i] = n->pointers[j];
      neighbor->num_keys++;
    }
    neighbor->pointers[order - 1] = n->pointers[order - 1];
  }

  root = delete_entry(root, n->parent, k_prime, n);
  free(n->keys);
  free(n->pointers);
  free(n);
  return root;
}

node *redistribute_nodes(node *root, node *n, node *neighbor, int neighbor_index,
             int k_prime_index, int k_prime) {
  int i;
  node *tmp;

  if (neighbor_index != -1) {
    if (!n->is_leaf)
      n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
    for (i = n->num_keys; i > 0; i--) {
      n->keys[i] = n->keys[i - 1];
      n->pointers[i] = n->pointers[i - 1];
    }
    if (!n->is_leaf) {
      n->pointers[0] = neighbor->pointers[neighbor->num_keys];
      tmp = (node *)n->pointers[0];
      tmp->parent = n;
      neighbor->pointers[neighbor->num_keys] = NULL;
      n->keys[0] = k_prime;
      n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
    } else {
      n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
      neighbor->pointers[neighbor->num_keys - 1] = NULL;
      n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
      n->parent->keys[k_prime_index] = n->keys[0];
    }
  }

  else {
    if (n->is_leaf) {
      n->keys[n->num_keys] = neighbor->keys[0];
      n->pointers[n->num_keys] = neighbor->pointers[0];
      n->parent->keys[k_prime_index] = neighbor->keys[1];
    } else {
      n->keys[n->num_keys] = k_prime;
      n->pointers[n->num_keys + 1] = neighbor->pointers[0];
      tmp = (node *)n->pointers[n->num_keys + 1];
      tmp->parent = n;
      n->parent->keys[k_prime_index] = neighbor->keys[0];
    }
    for (i = 0; i < neighbor->num_keys - 1; i++) {
      neighbor->keys[i] = neighbor->keys[i + 1];
      neighbor->pointers[i] = neighbor->pointers[i + 1];
    }
    if (!n->is_leaf)
      neighbor->pointers[i] = neighbor->pointers[i + 1];
  }

  n->num_keys++;
  neighbor->num_keys--;

  return root;
}

node *delete_entry(node *root, node *n, int key, void *pointer) {
  int min_keys;
  node *neighbor;
  int neighbor_index;
  int k_prime_index, k_prime;
  int capacity;

  n = remove_entry_from_node(n, key, pointer);

  if (n == root)
    return adjust_root(root);

  min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

  if (n->num_keys >= min_keys)
    return root;

  neighbor_index = get_neighbor_index(n);
  k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
  k_prime = n->parent->keys[k_prime_index];
  neighbor = neighbor_index == -1 ? n->parent->pointers[1] : n->parent->pointers[neighbor_index];

  capacity = n->is_leaf ? order : order - 1;

  if (neighbor->num_keys + n->num_keys < capacity)
    return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);
  else
    return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}

node *delete (node *root, int key) {
  node *key_leaf = NULL;
  record *key_record = NULL;

  key_record = find(root, key, false, &key_leaf);

  if (key_record != NULL && key_leaf != NULL) {
    root = delete_entry(root, key_leaf, key, key_record);
    free(key_record);
  }
  return root;
}

void destroy_tree_nodes(node *root) {
  int i;
  if (root->is_leaf)
    for (i = 0; i < root->num_keys; i++)
      free(root->pointers[i]);
  else
    for (i = 0; i < root->num_keys + 1; i++)
      destroy_tree_nodes(root->pointers[i]);
  free(root->pointers);
  free(root->keys);
  free(root);
}

node *destroy_tree(node *root) {
  destroy_tree_nodes(root);
  return NULL;
}

int main() {
  node *root;
  char instruction;

  root = NULL;

  root = insert(root, 5, 33);
  root = insert(root, 15, 21);
  root = insert(root, 25, 31);
  root = insert(root, 35, 41);
  root = insert(root, 45, 10);

  print_tree(root);

  root = delete (root, 5);

  print_tree(root);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////



/*
https://www.programiz.com/dsa/deletion-from-a-b-plus-tree
*/
// Deletion operation on a B+ tree in C++

#include <climits>
#include <fstream>
#include <iostream>
#include <sstream>
using namespace std;
int MAX = 3;

class BPTree;
class Node {
  bool IS_LEAF;
  int *key, size;
  Node **ptr;
  friend class BPTree;

   public:
  Node();
};
class BPTree {
  Node *root;
  void insertInternal(int, Node *, Node *);
  void removeInternal(int, Node *, Node *);
  Node *findParent(Node *, Node *);

   public:
  BPTree();
  void search(int);
  void insert(int);
  void remove(int);
  void display(Node *);
  Node *getRoot();
};
Node::Node() {
  key = new int[MAX];
  ptr = new Node *[MAX + 1];
}
BPTree::BPTree() {
  root = NULL;
}
void BPTree::insert(int x) {
  if (root == NULL) {
    root = new Node;
    root->key[0] = x;
    root->IS_LEAF = true;
    root->size = 1;
  } else {
    Node *cursor = root;
    Node *parent;
    while (cursor->IS_LEAF == false) {
      parent = cursor;
      for (int i = 0; i < cursor->size; i++) {
        if (x < cursor->key[i]) {
          cursor = cursor->ptr[i];
          break;
        }
        if (i == cursor->size - 1) {
          cursor = cursor->ptr[i + 1];
          break;
        }
      }
    }
    if (cursor->size < MAX) {
      int i = 0;
      while (x > cursor->key[i] && i < cursor->size)
        i++;
      for (int j = cursor->size; j > i; j--) {
        cursor->key[j] = cursor->key[j - 1];
      }
      cursor->key[i] = x;
      cursor->size++;
      cursor->ptr[cursor->size] = cursor->ptr[cursor->size - 1];
      cursor->ptr[cursor->size - 1] = NULL;
    } else {
      Node *newLeaf = new Node;
      int virtualNode[MAX + 1];
      for (int i = 0; i < MAX; i++) {
        virtualNode[i] = cursor->key[i];
      }
      int i = 0, j;
      while (x > virtualNode[i] && i < MAX)
        i++;
      for (int j = MAX + 1; j > i; j--) {
        virtualNode[j] = virtualNode[j - 1];
      }
      virtualNode[i] = x;
      newLeaf->IS_LEAF = true;
      cursor->size = (MAX + 1) / 2;
      newLeaf->size = MAX + 1 - (MAX + 1) / 2;
      cursor->ptr[cursor->size] = newLeaf;
      newLeaf->ptr[newLeaf->size] = cursor->ptr[MAX];
      cursor->ptr[MAX] = NULL;
      for (i = 0; i < cursor->size; i++) {
        cursor->key[i] = virtualNode[i];
      }
      for (i = 0, j = cursor->size; i < newLeaf->size; i++, j++) {
        newLeaf->key[i] = virtualNode[j];
      }
      if (cursor == root) {
        Node *newRoot = new Node;
        newRoot->key[0] = newLeaf->key[0];
        newRoot->ptr[0] = cursor;
        newRoot->ptr[1] = newLeaf;
        newRoot->IS_LEAF = false;
        newRoot->size = 1;
        root = newRoot;
      } else {
        insertInternal(newLeaf->key[0], parent, newLeaf);
      }
    }
  }
}
void BPTree::insertInternal(int x, Node *cursor, Node *child) {
  if (cursor->size < MAX) {
    int i = 0;
    while (x > cursor->key[i] && i < cursor->size)
      i++;
    for (int j = cursor->size; j > i; j--) {
      cursor->key[j] = cursor->key[j - 1];
    }
    for (int j = cursor->size + 1; j > i + 1; j--) {
      cursor->ptr[j] = cursor->ptr[j - 1];
    }
    cursor->key[i] = x;
    cursor->size++;
    cursor->ptr[i + 1] = child;
  } else {
    Node *newInternal = new Node;
    int virtualKey[MAX + 1];
    Node *virtualPtr[MAX + 2];
    for (int i = 0; i < MAX; i++) {
      virtualKey[i] = cursor->key[i];
    }
    for (int i = 0; i < MAX + 1; i++) {
      virtualPtr[i] = cursor->ptr[i];
    }
    int i = 0, j;
    while (x > virtualKey[i] && i < MAX)
      i++;
    for (int j = MAX + 1; j > i; j--) {
      virtualKey[j] = virtualKey[j - 1];
    }
    virtualKey[i] = x;
    for (int j = MAX + 2; j > i + 1; j--) {
      virtualPtr[j] = virtualPtr[j - 1];
    }
    virtualPtr[i + 1] = child;
    newInternal->IS_LEAF = false;
    cursor->size = (MAX + 1) / 2;
    newInternal->size = MAX - (MAX + 1) / 2;
    for (i = 0, j = cursor->size + 1; i < newInternal->size; i++, j++) {
      newInternal->key[i] = virtualKey[j];
    }
    for (i = 0, j = cursor->size + 1; i < newInternal->size + 1; i++, j++) {
      newInternal->ptr[i] = virtualPtr[j];
    }
    if (cursor == root) {
      Node *newRoot = new Node;
      newRoot->key[0] = cursor->key[cursor->size];
      newRoot->ptr[0] = cursor;
      newRoot->ptr[1] = newInternal;
      newRoot->IS_LEAF = false;
      newRoot->size = 1;
      root = newRoot;
    } else {
      insertInternal(cursor->key[cursor->size], findParent(root, cursor), newInternal);
    }
  }
}
Node *BPTree::findParent(Node *cursor, Node *child) {
  Node *parent;
  if (cursor->IS_LEAF || (cursor->ptr[0])->IS_LEAF) {
    return NULL;
  }
  for (int i = 0; i < cursor->size + 1; i++) {
    if (cursor->ptr[i] == child) {
      parent = cursor;
      return parent;
    } else {
      parent = findParent(cursor->ptr[i], child);
      if (parent != NULL)
        return parent;
    }
  }
  return parent;
}
void BPTree::remove(int x) {
  if (root == NULL) {
    cout << "Tree empty\n";
  } else {
    Node *cursor = root;
    Node *parent;
    int leftSibling, rightSibling;
    while (cursor->IS_LEAF == false) {
      for (int i = 0; i < cursor->size; i++) {
        parent = cursor;
        leftSibling = i - 1;
        rightSibling = i + 1;
        if (x < cursor->key[i]) {
          cursor = cursor->ptr[i];
          break;
        }
        if (i == cursor->size - 1) {
          leftSibling = i;
          rightSibling = i + 2;
          cursor = cursor->ptr[i + 1];
          break;
        }
      }
    }
    bool found = false;
    int pos;
    for (pos = 0; pos < cursor->size; pos++) {
      if (cursor->key[pos] == x) {
        found = true;
        break;
      }
    }
    if (!found) {
      cout << "Not found\n";
      return;
    }
    for (int i = pos; i < cursor->size; i++) {
      cursor->key[i] = cursor->key[i + 1];
    }
    cursor->size--;
    if (cursor == root) {
      for (int i = 0; i < MAX + 1; i++) {
        cursor->ptr[i] = NULL;
      }
      if (cursor->size == 0) {
        cout << "Tree died\n";
        delete[] cursor->key;
        delete[] cursor->ptr;
        delete cursor;
        root = NULL;
      }
      return;
    }
    cursor->ptr[cursor->size] = cursor->ptr[cursor->size + 1];
    cursor->ptr[cursor->size + 1] = NULL;
    if (cursor->size >= (MAX + 1) / 2) {
      return;
    }
    if (leftSibling >= 0) {
      Node *leftNode = parent->ptr[leftSibling];
      if (leftNode->size >= (MAX + 1) / 2 + 1) {
        for (int i = cursor->size; i > 0; i--) {
          cursor->key[i] = cursor->key[i - 1];
        }
        cursor->size++;
        cursor->ptr[cursor->size] = cursor->ptr[cursor->size - 1];
        cursor->ptr[cursor->size - 1] = NULL;
        cursor->key[0] = leftNode->key[leftNode->size - 1];
        leftNode->size--;
        leftNode->ptr[leftNode->size] = cursor;
        leftNode->ptr[leftNode->size + 1] = NULL;
        parent->key[leftSibling] = cursor->key[0];
        return;
      }
    }
    if (rightSibling <= parent->size) {
      Node *rightNode = parent->ptr[rightSibling];
      if (rightNode->size >= (MAX + 1) / 2 + 1) {
        cursor->size++;
        cursor->ptr[cursor->size] = cursor->ptr[cursor->size - 1];
        cursor->ptr[cursor->size - 1] = NULL;
        cursor->key[cursor->size - 1] = rightNode->key[0];
        rightNode->size--;
        rightNode->ptr[rightNode->size] = rightNode->ptr[rightNode->size + 1];
        rightNode->ptr[rightNode->size + 1] = NULL;
        for (int i = 0; i < rightNode->size; i++) {
          rightNode->key[i] = rightNode->key[i + 1];
        }
        parent->key[rightSibling - 1] = rightNode->key[0];
        return;
      }
    }
    if (leftSibling >= 0) {
      Node *leftNode = parent->ptr[leftSibling];
      for (int i = leftNode->size, j = 0; j < cursor->size; i++, j++) {
        leftNode->key[i] = cursor->key[j];
      }
      leftNode->ptr[leftNode->size] = NULL;
      leftNode->size += cursor->size;
      leftNode->ptr[leftNode->size] = cursor->ptr[cursor->size];
      removeInternal(parent->key[leftSibling], parent, cursor);
      delete[] cursor->key;
      delete[] cursor->ptr;
      delete cursor;
    } else if (rightSibling <= parent->size) {
      Node *rightNode = parent->ptr[rightSibling];
      for (int i = cursor->size, j = 0; j < rightNode->size; i++, j++) {
        cursor->key[i] = rightNode->key[j];
      }
      cursor->ptr[cursor->size] = NULL;
      cursor->size += rightNode->size;
      cursor->ptr[cursor->size] = rightNode->ptr[rightNode->size];
      cout << "Merging two leaf nodes\n";
      removeInternal(parent->key[rightSibling - 1], parent, rightNode);
      delete[] rightNode->key;
      delete[] rightNode->ptr;
      delete rightNode;
    }
  }
}
void BPTree::removeInternal(int x, Node *cursor, Node *child) {
  if (cursor == root) {
    if (cursor->size == 1) {
      if (cursor->ptr[1] == child) {
        delete[] child->key;
        delete[] child->ptr;
        delete child;
        root = cursor->ptr[0];
        delete[] cursor->key;
        delete[] cursor->ptr;
        delete cursor;
        cout << "Changed root node\n";
        return;
      } else if (cursor->ptr[0] == child) {
        delete[] child->key;
        delete[] child->ptr;
        delete child;
        root = cursor->ptr[1];
        delete[] cursor->key;
        delete[] cursor->ptr;
        delete cursor;
        cout << "Changed root node\n";
        return;
      }
    }
  }
  int pos;
  for (pos = 0; pos < cursor->size; pos++) {
    if (cursor->key[pos] == x) {
      break;
    }
  }
  for (int i = pos; i < cursor->size; i++) {
    cursor->key[i] = cursor->key[i + 1];
  }
  for (pos = 0; pos < cursor->size + 1; pos++) {
    if (cursor->ptr[pos] == child) {
      break;
    }
  }
  for (int i = pos; i < cursor->size + 1; i++) {
    cursor->ptr[i] = cursor->ptr[i + 1];
  }
  cursor->size--;
  if (cursor->size >= (MAX + 1) / 2 - 1) {
    return;
  }
  if (cursor == root)
    return;
  Node *parent = findParent(root, cursor);
  int leftSibling, rightSibling;
  for (pos = 0; pos < parent->size + 1; pos++) {
    if (parent->ptr[pos] == cursor) {
      leftSibling = pos - 1;
      rightSibling = pos + 1;
      break;
    }
  }
  if (leftSibling >= 0) {
    Node *leftNode = parent->ptr[leftSibling];
    if (leftNode->size >= (MAX + 1) / 2) {
      for (int i = cursor->size; i > 0; i--) {
        cursor->key[i] = cursor->key[i - 1];
      }
      cursor->key[0] = parent->key[leftSibling];
      parent->key[leftSibling] = leftNode->key[leftNode->size - 1];
      for (int i = cursor->size + 1; i > 0; i--) {
        cursor->ptr[i] = cursor->ptr[i - 1];
      }
      cursor->ptr[0] = leftNode->ptr[leftNode->size];
      cursor->size++;
      leftNode->size--;
      return;
    }
  }
  if (rightSibling <= parent->size) {
    Node *rightNode = parent->ptr[rightSibling];
    if (rightNode->size >= (MAX + 1) / 2) {
      cursor->key[cursor->size] = parent->key[pos];
      parent->key[pos] = rightNode->key[0];
      for (int i = 0; i < rightNode->size - 1; i++) {
        rightNode->key[i] = rightNode->key[i + 1];
      }
      cursor->ptr[cursor->size + 1] = rightNode->ptr[0];
      for (int i = 0; i < rightNode->size; ++i) {
        rightNode->ptr[i] = rightNode->ptr[i + 1];
      }
      cursor->size++;
      rightNode->size--;
      return;
    }
  }
  if (leftSibling >= 0) {
    Node *leftNode = parent->ptr[leftSibling];
    leftNode->key[leftNode->size] = parent->key[leftSibling];
    for (int i = leftNode->size + 1, j = 0; j < cursor->size; j++) {
      leftNode->key[i] = cursor->key[j];
    }
    for (int i = leftNode->size + 1, j = 0; j < cursor->size + 1; j++) {
      leftNode->ptr[i] = cursor->ptr[j];
      cursor->ptr[j] = NULL;
    }
    leftNode->size += cursor->size + 1;
    cursor->size = 0;
    removeInternal(parent->key[leftSibling], parent, cursor);
  } else if (rightSibling <= parent->size) {
    Node *rightNode = parent->ptr[rightSibling];
    cursor->key[cursor->size] = parent->key[rightSibling - 1];
    for (int i = cursor->size + 1, j = 0; j < rightNode->size; j++) {
      cursor->key[i] = rightNode->key[j];
    }
    for (int i = cursor->size + 1, j = 0; j < rightNode->size + 1; j++) {
      cursor->ptr[i] = rightNode->ptr[j];
      rightNode->ptr[j] = NULL;
    }
    cursor->size += rightNode->size + 1;
    rightNode->size = 0;
    removeInternal(parent->key[rightSibling - 1], parent, rightNode);
  }
}
void BPTree::display(Node *cursor) {
  if (cursor != NULL) {
    for (int i = 0; i < cursor->size; i++) {
      cout << cursor->key[i] << " ";
    }
    cout << "\n";
    if (cursor->IS_LEAF != true) {
      for (int i = 0; i < cursor->size + 1; i++) {
        display(cursor->ptr[i]);
      }
    }
  }
}
Node *BPTree::getRoot() {
  return root;
}

int main() {
  BPTree node;
  node.insert(5);
  node.insert(15);
  node.insert(25);
  node.insert(35);
  node.insert(45);

  node.display(node.getRoot());

  node.remove(15);

  node.display(node.getRoot());
}






////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////

// https://www.educba.com/b-plus-tree-deletion/


import math
# Node creation
class Node:
def __init__(self, order):
self.order = order
self.values = [] self.keys = [] self.nextKey = None
self.parent = None
self.check_leaf = False
# Insert at the leaf
def insert_at_leaf(self, leaf, value, key):
if (self.values):
temp1 = self.values
for i in range(len(temp1)):
if (value == temp1[i]):
self.keys[i].append(key)
break
elif (value < temp1[i]):
self.values = self.values[:i] + [value] + self.values[i:] self.keys = self.keys[:i] + [[key]] + self.keys[i:] break
elif (i + 1 == len(temp1)):
self.values.append(value)
self.keys.append([key])
break
else:
self.values = [value] self.keys = [[key]] # B plus tree
class BplusTree:
def __init__(self, order):
self.root = Node(order)
self.root.check_leaf = True
# Insert operation
def insert(self, value, key):
value = str(value)
old_node = self.search(value)
old_node.insert_at_leaf(old_node, value, key)
if (len(old_node.values) == old_node.order):
node1 = Node(old_node.order)
node1.check_leaf = True
node1.parent = old_node.parent
mid = int(math.ceil(old_node.order / 2)) - 1
node1.values = old_node.values[mid + 1:] node1.keys = old_node.keys[mid + 1:] node1.nextKey = old_node.nextKey
old_node.values = old_node.values[:mid + 1] old_node.keys = old_node.keys[:mid + 1] old_node.nextKey = node1
self.insert_in_parent(old_node, node1.values[0], node1)
# Search operation for different operations
def search(self, value):
current_node = self.root
while(current_node.check_leaf == False):
temp2 = current_node.values
for i in range(len(temp2)):
if (value == temp2[i]):
current_node = current_node.keys[i + 1] break
elif (value < temp2[i]):
current_node = current_node.keys[i] break
elif (i + 1 == len(current_node.values)):
current_node = current_node.keys[i + 1] break
return current_node
# Find the node
def find(self, value, key):
l = self.search(value)
for i, item in enumerate(l.values):
if item == value:
if key in l.keys[i]:
return True
else:
return False
return False
# Inserting at the parent
def insert_in_parent(self, n, value, ndash):
if (self.root == n):
rootNode = Node(n.order)
rootNode.values = [value] rootNode.keys = [n, ndash] self.root = rootNode
n.parent = rootNode
ndash.parent = rootNode
return
parentNode = n.parent
temp3 = parentNode.keys
for i in range(len(temp3)):
if (temp3[i] == n):
parentNode.values = parentNode.values[:i] + \
[value] + parentNode.values[i:] parentNode.keys = parentNode.keys[:i +
1] + [ndash] + parentNode.keys[i + 1:] if (len(parentNode.keys) > parentNode.order):
parentdash = Node(parentNode.order)
parentdash.parent = parentNode.parent
mid = int(math.ceil(parentNode.order / 2)) - 1
parentdash.values = parentNode.values[mid + 1:] parentdash.keys = parentNode.keys[mid + 1:] value_ = parentNode.values[mid] if (mid == 0):
parentNode.values = parentNode.values[:mid + 1] else:
parentNode.values = parentNode.values[:mid] parentNode.keys = parentNode.keys[:mid + 1] for j in parentNode.keys:
j.parent = parentNode
for j in parentdash.keys:
j.parent = parentdash
self.insert_in_parent(parentNode, value_, parentdash)
# Delete a node
def delete(self, value, key):
node_ = self.search(value)
temp = 0
for i, item in enumerate(node_.values):
if item == value:
temp = 1
if key in node_.keys[i]:
if len(node_.keys[i]) > 1:
node_.keys[i].pop(node_.keys[i].index(key))
elif node_ == self.root:
node_.values.pop(i)
node_.keys.pop(i)
else:
node_.keys[i].pop(node_.keys[i].index(key))
del node_.keys[i] node_.values.pop(node_.values.index(value))
self.deleteEntry(node_, value, key)
else:
print("Value not in Key")
return
if temp == 0:
print("Value not in Tree")
return
# Delete an entry
def deleteEntry(self, node_, value, key):
if not node_.check_leaf:
for i, item in enumerate(node_.keys):
if item == key:
node_.keys.pop(i)
break
for i, item in enumerate(node_.values):
if item == value:
node_.values.pop(i)
break
if self.root == node_ and len(node_.keys) == 1:
self.root = node_.keys[0] node_.keys[0].parent = None
del node_
return
elif (len(node_.keys) < int(math.ceil(node_.order / 2)) and node_.check_leaf == False) or (len(node_.values) < int(math.ceil((node_.order - 1) / 2)) and node_.check_leaf == True):
is_predecessor = 0
parentNode = node_.parent
PrevNode = -1
NextNode = -1
PrevK = -1
PostK = -1
for i, item in enumerate(parentNode.keys):
if item == node_:
if i > 0:
PrevNode = parentNode.keys[i - 1] PrevK = parentNode.values[i - 1] if i < len(parentNode.keys) - 1:
NextNode = parentNode.keys[i + 1] PostK = parentNode.values[i] if PrevNode == -1:
ndash = NextNode
value_ = PostK
elif NextNode == -1:
is_predecessor = 1
ndash = PrevNode
value_ = PrevK
else:
if len(node_.values) + len(NextNode.values) < node_.order:
ndash = NextNode
value_ = PostK
else:
is_predecessor = 1
ndash = PrevNode
value_ = PrevK
if len(node_.values) + len(ndash.values) < node_.order:
if is_predecessor == 0:
node_, ndash = ndash, node_
ndash.keys += node_.keys
if not node_.check_leaf:
ndash.values.append(value_)
else:
ndash.nextKey = node_.nextKey
ndash.values += node_.values
if not ndash.check_leaf:
for j in ndash.keys:
j.parent = ndash
self.deleteEntry(node_.parent, value_, node_)
del node_
else:
if is_predecessor == 1:
if not node_.check_leaf:
ndashpm = ndash.keys.pop(-1)
ndashkm_1 = ndash.values.pop(-1)
node_.keys = [ndashpm] + node_.keys
node_.values = [value_] + node_.values
parentNode = node_.parent
for i, item in enumerate(parentNode.values):
if item == value_:
p.values[i] = ndashkm_1
break
else:
ndashpm = ndash.keys.pop(-1)
ndashkm = ndash.values.pop(-1)
node_.keys = [ndashpm] + node_.keys
node_.values = [ndashkm] + node_.values
parentNode = node_.parent
for i, item in enumerate(p.values):
if item == value_:
parentNode.values[i] = ndashkm
break
else:
if not node_.check_leaf:
ndashp0 = ndash.keys.pop(0)
ndashk0 = ndash.values.pop(0)
node_.keys = node_.keys + [ndashp0] node_.values = node_.values + [value_] parentNode = node_.parent
for i, item in enumerate(parentNode.values):
if item == value_:
parentNode.values[i] = ndashk0
break
else:
ndashp0 = ndash.keys.pop(0)
ndashk0 = ndash.values.pop(0)
node_.keys = node_.keys + [ndashp0] node_.values = node_.values + [ndashk0] parentNode = node_.parent
for i, item in enumerate(parentNode.values):
if item == value_:
parentNode.values[i] = ndash.values[0] break
if not ndash.check_leaf:
for j in ndash.keys:
j.parent = ndash
if not node_.check_leaf:
for j in node_.keys:
j.parent = node_
if not parentNode.check_leaf:
for j in parentNode.keys:
j.parent = parentNode
# Print the tree
def printTree(tree):
lst = [tree.root] level = [0] leaf = None
flag = 0
lev_leaf = 0
node1 = Node(str(level[0]) + str(tree.root.values))
while (len(lst) != 0):
x = lst.pop(0)
lev = level.pop(0)
if (x.check_leaf == False):
for i, item in enumerate(x.keys):
print(item.values)
else:
for i, item in enumerate(x.keys):
print(item.values)
if (flag == 0):
lev_leaf = lev
leaf = x
flag = 1
record_len = 3
bplustree = BplusTree(record_len)
bplustree.insert('5', '33')
bplustree.insert('15', '21')
bplustree.insert('25', '31')
bplustree.insert('35', '41')
bplustree.insert('45', '10')
printTree(bplustree)
print()
if(bplustree.find('5', '33')):
print("Found")
else:
print("Not found")
print()
if(bplustree.find('5', '34')):
print("Found")
else:
print("Not found")