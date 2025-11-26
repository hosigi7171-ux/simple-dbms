#include "db_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int global_table_id;

int main(int argc, char **argv) {
  char db_pathname[VALUE_SIZE] = "sample.db";

  int64_t input_key;
  char input_value[VALUE_SIZE];
  int64_t range;
  char instruction;

  license_notice();
  usage();

  printf("> ");
  while (scanf("%c", &instruction) != EOF) {

    if (instruction == '\n' || instruction == ' ') {
      continue;
    }

    switch (instruction) {
    case 'o':
      if (scanf("%s", db_pathname) == 1) {
        if (global_table_id >= 0) {
          close_table();
        }

        global_table_id = open_table(db_pathname);
        if (global_table_id == FAILURE) {
          perror("Failure to open database file");
          global_table_id = -1;
        } else {
          printf("Successfully open\n");
        }
      } else {
        printf("Missing pathname 'o'\n");
      }
      break;

    case 'i': // Insert
    case 'd': // Delete
    case 'f': // Find
    case 'r': // Range Search
    case 't': // Print Tree

      if (global_table_id < 0) {
        printf("Table not open Use 'o <pathname>' first\n");
      } else {
        switch (instruction) {
        case 'i':
          scanf("%" PRId64, &input_key);
          snprintf(input_value, VALUE_SIZE, "%" PRId64 "_value", input_key);

          if (db_insert(input_key, input_value) == SUCCESS) {
            printf("Key %" PRId64 " inserted successfully with value: %s\n",
                   input_key, input_value);
          } else {
            printf("Insertion failed for key %" PRId64 ".\n", input_key);
          }
          break;

        case 'd':
          scanf("%" PRId64, &input_key);
          if (db_delete(input_key) == SUCCESS) {
            printf("Key %" PRId64 " deletion success\n", input_key);
          } else {
            printf("Key %" PRId64 " deletion fail\n", input_key);
          }
          break;

        case 'f':
          scanf("%" PRId64, &input_key);
          char ret_val[VALUE_SIZE];
          if (db_find(input_key, ret_val) == SUCCESS) {
            printf("Key %" PRId64 " found. Value: %s\n", input_key, ret_val);
          } else {
            printf("Key %" PRId64 " not found.\n", input_key);
          }
          break;

        case 'r':
          scanf("%" PRId64 " %" PRId64, &input_key, &range);
          if (input_key > range) {
            int64_t tmp = range;
            range = input_key;
            input_key = tmp;
          }
          if (db_find_and_print_range(input_key, range) != SUCCESS) {
            printf("please check range\n");
          }
          break;

        case 't':
          db_print_tree();
          break;
        }
      }
      break;

    case 'q':
      while (getchar() != '\n')
        ;
      if (global_table_id >= 0) {
        close_table();
      }
      printf("Exiting program.\n");
      return EXIT_SUCCESS;
      break;

    case '?':
      usage();
      break;

    default:
      printf("Unknown command '%c'. Supported: o, i, d, f, r, t, q\n",
             instruction);
      break;
    }

    while (getchar() != (int)'\n')
      ;

    printf("> ");
  }
  printf("\n");

  return EXIT_SUCCESS;
}
