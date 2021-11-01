#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <cstring>
#include <unistd.h>

#include "table.h"
#include "type.h"

using namespace db::table;
using namespace db::type;

inline void print_prompt()
{
    printf("db > ");
}

inline void print_row(const Row &row)
{
    printf("(%d, %s, %s)\n", row.id, row.username, row.email);
}

inline void read_input(InputBuffer &buffer)
{
    auto bytes_read = getline(&(buffer.buffer), &(buffer.buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // -1 is \0
    buffer.input_length = bytes_read - 1;
    buffer.buffer[bytes_read - 1] = 0;
}

inline PrepareResult prepare_statement(InputBuffer &input_buffer, Statement &statement)
{
    if (std::strncmp(input_buffer.buffer, "insert", 6) == 0) {
        statement.type = StatementType::STATEMENT_INSERT;
        auto &statement_row = statement.row_to_insert;
        auto args_assigned = std::sscanf(input_buffer.buffer, "insert %d %s %s",
                                         &(statement_row.id), statement_row.username, statement_row.email);
        if (args_assigned < 3) {
            return PrepareResult::PREPARE_SYNTAX_ERROR;
        }
        return PrepareResult::PREPARE_SUCCESS;
    }

    if (std::strncmp(input_buffer.buffer, "select", 6) == 0) {
        statement.type = StatementType::STATEMENT_SELECT;
        return PrepareResult::PREPARE_SUCCESS;
    }

    return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT;
}

inline ExecuteResult execute_insert(const Statement &statement, Table &table)
{
    if (table.num_rows >= TABLE_MAX_ROWS) {
        return ExecuteResult::EXECUTE_TABLE_FULL;
    }

    auto &row_to_insert = statement.row_to_insert;
    Cursor cursor = table_end(table);
    // save to table page
    // serialize_row(&row_to_insert, row_slot(table, table.num_rows));
    serialize_row(&row_to_insert, cursor_value(cursor));
    table.num_rows += 1;

    return ExecuteResult::EXECUTE_SUCCESS;
}

inline ExecuteResult execute_select(const Statement &statement, Table &table)
{
    Cursor cursor = table_start(table);

    Row row{};
    while (!(cursor.end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(row);
        cursor_advance(cursor);
    }

    return ExecuteResult::EXECUTE_SUCCESS;
}

inline ExecuteResult execute_statement(const Statement &statement, Table &table)
{
    switch (statement.type) {
        case StatementType::STATEMENT_INSERT:
            return execute_insert(statement, table);
        case StatementType::STATEMENT_SELECT:
            return execute_select(statement, table);
    }
}

Table db_open(const char* filename)
{
    Pager pager = pager_open(filename);
    uint32_t num_rows = pager.file_length / ROW_SIZE;

    Table table{};
    table.num_rows = num_rows;
    table.pager = pager;

    return std::move(table);
}

void db_close(Table& table)
{
    Pager pager = table.pager;
    uint32_t num_full_pages = table.num_rows / ROWS_PER_PAGE;
    
    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager.pages[i] == nullptr) continue;

        pager_flush(pager, i, PAGE_SIZE);
        std::free(pager.pages[i]);
        pager.pages[i] = nullptr;
    }

    uint32_t num_additional_rows = table.num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager.pages[page_num] != nullptr) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            std::free(pager.pages[page_num]);
            pager.pages[page_num] = nullptr;
        }
    }

    int result = close(pager.file_descriptor);;
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager.pages[i];
        if (page) {
            std::free(page);
            pager.pages[i] = nullptr;
        }
    }
}

inline MetaCommandResult do_meta_command(InputBuffer &buffer, Table& table)
{
    if (std::strcmp(buffer.buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    return MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND;
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table table = db_open(filename);
    InputBuffer input_buffer{};

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (std::strcmp(input_buffer.buffer, ".exit") == 0) {
            if (input_buffer.buffer[0] == '.') {
                switch (do_meta_command(input_buffer, table)) {
                    case (MetaCommandResult::META_COMMAND_SUCCESS):
                        continue;
                    case (MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND):
                        printf("Unrecognized command '%s'.\n", input_buffer.buffer);
                        continue;
                }
            }
        }

        Statement statement{};
        switch (prepare_statement(input_buffer, statement)) {
            case (PrepareResult::PREPARE_SUCCESS):
                break;
            case (PrepareResult::PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer.buffer);
                continue;
        }

        switch (execute_statement(statement, table)) {
            case ExecuteResult::EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case ExecuteResult::EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
        }
    }

    return 0;
}
