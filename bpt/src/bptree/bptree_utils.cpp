#include "bpt.h"
#include "bpt_internal.h"

#define Version "1.14"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.

 *  3. Neither the name of the copyright holder nor the names of its
 *  contributors may be used to endorse or promote products derived from this
 *  software without specific prior written permission.

 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.

 *  Author:  Amittai Aviram
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

// GLOBALS.

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
queue *q_head;

// OUTPUT AND UTILITIES

/* Copyright and license notice to user at startup.
 */
void license_notice(void) {
  printf("bpt version %s -- Copyright (C) 2010  Amittai Aviram "
         "http://www.amittai.com\n",
         Version);
  printf("This program comes with ABSOLUTELY NO WARRANTY; for details "
         "type `show w'.\n"
         "This is free software, and you are welcome to redistribute it\n"
         "under certain conditions; type `show c' for details.\n\n");
}

/* Routine to print portion of GPL license to stdout.
 */
void print_license(int license_part) {
  int start, end, line;
  FILE *fp;
  char buffer[0x100];

  switch (license_part) {
  case LICENSE_WARRANTEE:
    start = LICENSE_WARRANTEE_START;
    end = LICENSE_WARRANTEE_END;
    break;
  case LICENSE_CONDITIONS:
    start = LICENSE_CONDITIONS_START;
    end = LICENSE_CONDITIONS_END;
    break;
  default:
    return;
  }

  fp = fopen(LICENSE_FILE, "r");
  if (fp == NULL) {
    perror("print_license: fopen");
    exit(EXIT_FAILURE);
  }
  for (line = 0; line < start; line++)
    fgets(buffer, sizeof(buffer), fp);
  for (; line < end; line++) {
    fgets(buffer, sizeof(buffer), fp);
    printf("%s", buffer);
  }
  fclose(fp);
}

void usage(void) {
  printf("Simple DBMS (B+ Tree based) - Commands:\n\n");
  printf("  o <filename>   Open (or create) a database file\n");
  printf("  i <key>        Insert key (value is automatically generated as "
         "\"<key>_value\")\n");
  printf("  f <key>        Find and print the value for <key>\n");
  printf("  d <key>        Delete <key> and its value\n");
  printf("  r <k1> <k2>    Print all keys and values in range [min(k1,k2) ... "
         "max(k1,k2)] (max range is 10000)\n");
  printf("  t              Print the entire B+ tree structure\n");
  printf("  q              Quit the program (closes the current table)\n");
  printf("  ?              Show this help message\n\n");
  printf("> ");
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
void enqueue(pagenum_t new_page_num, int level) {
  if (q_head == NULL) {
    q_head = (queue *)malloc(sizeof(queue));
    q_head->page_num = new_page_num;
    q_head->level = level;
    q_head->next = NULL;
  } else {
    queue *cur = q_head;
    while (cur->next != NULL) {
      cur = cur->next;
    }
    queue *new_node = (queue *)malloc(sizeof(queue));
    new_node->page_num = new_page_num;
    new_node->level = level;
    new_node->next = NULL;

    cur->next = new_node;
  }
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
queue *dequeue(void) {
  queue *next_node = q_head;
  q_head = q_head->next;
  next_node->next = NULL;
  return next_node;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut(int length) {
  if (length % 2 == 0)
    return length / 2;
  else
    return length / 2 + 1;
}
