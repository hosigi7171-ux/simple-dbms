#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db_api.h"

int main(int argc, char** argv) {
  char db_pathname[VALUE_SIZE] = "sample.db";

  tableid_t current_table_id = -1;

  int64_t input_key;
  char input_value[VALUE_SIZE];
  int64_t range;
  char instruction;

  license_notice();
  usage();

  if (init_db(100) != SUCCESS) {
    perror("Failed to initialize DB buffer");
    return EXIT_FAILURE;
  }

  printf("> ");
  while (scanf("%c", &instruction) != EOF) {
    if (instruction == '\n' || instruction == ' ') {
      continue;
    }

    switch (instruction) {
      case 'o':
        if (scanf("%s", db_pathname) == 1) {
          if (current_table_id >= 0) {
            close_table(current_table_id);
            current_table_id = -1;
          }
          current_table_id = open_table(db_pathname);
          if (current_table_id == FAILURE) {
            perror("Failure to open database file");
            current_table_id = -1;
          } else {
            printf("Successfully open\n");
          }
        } else {
          printf("Missing pathname 'o'\n");
        }
        break;

      case 'i':  // Insert
      case 'd':  // Delete
      case 'f':  // Find
      case 'r':  // Range Search
      case 't':  // Print Tree
      case 'c':  // Close Table

        if (current_table_id < 0) {
          printf("Table not open Use 'o <pathname>' first\n");
        } else {
          switch (instruction) {
            case 'i':
              scanf("%" PRId64, &input_key);
              snprintf(input_value, VALUE_SIZE, "%" PRId64 "_value", input_key);

              if (db_insert(current_table_id, input_key, input_value) ==
                  SUCCESS) {
                printf("Key %" PRId64 " inserted successfully with value: %s\n",
                       input_key, input_value);
              } else {
                printf("Insertion failed for key %" PRId64 ".\n", input_key);
              }
              break;

            case 'd':
              scanf("%" PRId64, &input_key);
              if (db_delete(current_table_id, input_key) == SUCCESS) {
                printf("Key %" PRId64 " deletion success\n", input_key);
              } else {
                printf("Key %" PRId64 " deletion fail\n", input_key);
              }
              break;

            case 'f':
              scanf("%" PRId64, &input_key);
              char ret_val[VALUE_SIZE];
              if (db_find(current_table_id, input_key, ret_val) == SUCCESS) {
                printf("Key %" PRId64 " found. Value: %s\n", input_key,
                       ret_val);
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
              if (db_find_and_print_range(current_table_id, input_key, range) !=
                  SUCCESS) {
                printf("please check range\n");
              }
              break;

            case 't':
              db_print_tree(current_table_id);
              break;

            case 'c':
              if (close_table(current_table_id) == SUCCESS) {
                printf("Table %d closed successfully.\n", current_table_id);
                current_table_id = -1;
              }
              break;
          }
        }
        break;

      case 'q':
        while (getchar() != '\n');
        if (current_table_id >= 0) {
          close_table(current_table_id);
        }
        shutdown_db();
        printf("Exiting program.\n");
        return EXIT_SUCCESS;
        break;

      case '?':
        usage();
        break;

      default:
        printf("Unknown command '%c'. Supported: o, i, d, f, r, t, c, q, ?\n",
               instruction);
        break;
    }

    while (getchar() != (int)'\n');

    printf("> ");
  }
  printf("\n");
  shutdown_db();

  return EXIT_SUCCESS;
}
